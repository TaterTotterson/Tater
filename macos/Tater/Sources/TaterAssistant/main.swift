import AppKit
import CryptoKit
import Darwin
import Foundation
import WebKit

private let taterPort = 8501
private let taterDataRootPathKey = "TaterDataRootPath"
private let taterDataRootBookmarkKey = "TaterDataRootBookmark"

private enum BackendState: Equatable {
    case stopped
    case bootstrapping
    case starting
    case running
    case failed(String)

    var label: String {
        switch self {
        case .stopped:
            return "Stopped"
        case .bootstrapping:
            return "Setting up"
        case .starting:
            return "Starting"
        case .running:
            return "Running"
        case .failed(let message):
            return "Failed: \(message)"
        }
    }
}

private final class BackendManager {
    let supportRoot: URL
    let appRoot: URL
    let venvDir: URL
    let pythonRoot: URL
    let managedPythonDir: URL
    let runtimeDir: URL
    private(set) var agentRoot: URL
    let logsDir: URL
    let sourceRoot: URL
    let webURL = URL(string: "http://127.0.0.1:\(taterPort)")!
    let nativeStatusToken = UUID().uuidString

    var onStateChange: ((BackendState) -> Void)?
    var onLogAppend: ((String) -> Void)?

    private(set) var state: BackendState = .stopped {
        didSet {
            DispatchQueue.main.async { [state, onStateChange] in
                onStateChange?(state)
            }
        }
    }

    private var process: Process?
    private var setupProcess: Process?
    private var logHandle: FileHandle?
    private var outputPipe: Pipe?
    private var selectedPythonPath: String?
    private var externalBackendPID: pid_t?
    private let gracefulStopTimeout: TimeInterval = 35
    private let descendantExitGrace: TimeInterval = 3
    private let lifecycleLock = NSLock()
    private var lifecycleOperationInProgress = false
    private var dataRootSecurityScope: URL?

    init() {
        let home = FileManager.default.homeDirectoryForCurrentUser
        supportRoot = home.appendingPathComponent(".taterassistant", isDirectory: true)
        appRoot = supportRoot.appendingPathComponent("app", isDirectory: true)
        venvDir = supportRoot.appendingPathComponent("venv", isDirectory: true)
        pythonRoot = supportRoot.appendingPathComponent("python", isDirectory: true)
        managedPythonDir = pythonRoot.appendingPathComponent("cpython-3.11", isDirectory: true)
        runtimeDir = supportRoot.appendingPathComponent("runtime", isDirectory: true)
        let defaultAgentRoot = supportRoot.appendingPathComponent("agent_lab", isDirectory: true)
        let resolvedDataRoot = BackendManager.resolveSavedDataRoot(defaultRoot: defaultAgentRoot)
        agentRoot = resolvedDataRoot.url
        dataRootSecurityScope = resolvedDataRoot.securityScope
        logsDir = supportRoot.appendingPathComponent("logs", isDirectory: true)
        sourceRoot = BackendManager.resolveSourceRoot(supportRoot: supportRoot)
    }

    deinit {
        dataRootSecurityScope?.stopAccessingSecurityScopedResource()
    }

    private static func resolveSavedDataRoot(defaultRoot: URL) -> (url: URL, securityScope: URL?) {
        let defaults = UserDefaults.standard
        guard let savedPath = defaults.string(forKey: taterDataRootPathKey), !savedPath.isEmpty else {
            return (defaultRoot, nil)
        }

        var scopeURL: URL?
        if let bookmark = defaults.data(forKey: taterDataRootBookmarkKey) {
            var stale = false
            if let resolved = try? URL(
                resolvingBookmarkData: bookmark,
                options: [.withSecurityScope],
                relativeTo: nil,
                bookmarkDataIsStale: &stale
            ) {
                if resolved.startAccessingSecurityScopedResource() {
                    scopeURL = resolved
                }
                if stale, let refreshed = try? resolved.bookmarkData(
                    options: [.withSecurityScope],
                    includingResourceValuesForKeys: nil,
                    relativeTo: nil
                ) {
                    defaults.set(refreshed, forKey: taterDataRootBookmarkKey)
                }
            }
        }
        return (URL(fileURLWithPath: savedPath, isDirectory: true), scopeURL)
    }

    func activateDataRoot(_ root: URL, securityScope: URL?, bookmark: Data?) {
        dataRootSecurityScope?.stopAccessingSecurityScopedResource()
        dataRootSecurityScope = securityScope
        agentRoot = root.standardizedFileURL
        let defaults = UserDefaults.standard
        defaults.set(agentRoot.path, forKey: taterDataRootPathKey)
        if let bookmark {
            defaults.set(bookmark, forKey: taterDataRootBookmarkKey)
        } else {
            defaults.removeObject(forKey: taterDataRootBookmarkKey)
        }
    }

    func restoreDataRoot(_ root: URL, bookmark: Data?) {
        var securityScope: URL?
        if let bookmark {
            var stale = false
            if let resolved = try? URL(
                resolvingBookmarkData: bookmark,
                options: [.withSecurityScope],
                relativeTo: nil,
                bookmarkDataIsStale: &stale
            ), resolved.startAccessingSecurityScopedResource() {
                securityScope = resolved
            }
        }
        activateDataRoot(root, securityScope: securityScope, bookmark: bookmark)
    }

    func start() {
        guard beginLifecycleOperation() else {
            appendLauncherLog("Start requested; another bootstrap or launch operation is already in progress.\n")
            return
        }

        if isManagedProcessRunning() {
            appendLauncherLog("Start requested; backend process is already running.\n")
            state = .running
            endLifecycleOperation()
            return
        }

        appendLauncherLog("Start requested.\n")
        state = .bootstrapping
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self else { return }
            defer {
                self.endLifecycleOperation()
            }
            do {
                self.cleanupStaleBundledLlamaServers()
                try self.validateDataRootAvailability()
                self.appendLauncherLog("Ensuring private folders.\n")
                try self.ensurePrivateFolders()
                self.appendLauncherLog("Ensuring Python runtime.\n")
                try self.ensurePythonRuntime()
                self.appendLauncherLog("Running setup check.\n")
                try self.bootstrapIfNeeded()
                if self.isWebReady() {
                    self.appendLog("Tater web UI is already running on \(self.webURL.absoluteString)\n")
                    self.appendLauncherLog("Web UI is already ready; marking running.\n")
                    self.state = .running
                    return
                }
                self.appendLauncherLog("Setup complete; launching backend.\n")
                try self.launchBackend()
            } catch {
                self.appendLauncherLog("Start failed: \(error.localizedDescription)\n")
                self.state = .failed(error.localizedDescription)
            }
        }
    }

    func restart() {
        appendLauncherLog("Restart requested.\n")
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self else { return }
            self.stop(waitForExit: true)
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                self.start()
            }
        }
    }

    func stop(waitForExit: Bool = false) {
        if !waitForExit {
            DispatchQueue.global(qos: .utility).async { [weak self] in
                self?.stop(waitForExit: true)
            }
            return
        }

        if let process, process.isRunning {
            let pid = process.processIdentifier
            let descendants = descendantProcessIDs(of: pid)
            appendLauncherLog("Stop requested for managed backend pid \(pid).\n")
            if Darwin.kill(pid, SIGTERM) != 0 {
                appendLauncherLog(
                    "Failed to send SIGTERM to managed backend pid \(pid): errno \(errno).\n"
                )
            }
            waitForManagedProcessExit(
                process,
                descendants: descendants,
                timeout: gracefulStopTimeout
            )
            cleanupStaleBundledLlamaServers()
            closeLogHandle()
            self.process = nil
            externalBackendPID = nil
            state = .stopped
            return
        }

        if let pid = backendProcessPIDOnPort() {
            appendLauncherLog("Stop requested for discovered backend pid \(pid).\n")
            terminateBackendProcess(pid: pid, waitForExit: true)
            cleanupStaleBundledLlamaServers()
            detachOutputPipe()
            closeLogHandle()
            process = nil
            externalBackendPID = nil
            state = .stopped
            return
        }

        detachOutputPipe()
        cleanupStaleBundledLlamaServers()
        closeLogHandle()
        process = nil
        externalBackendPID = nil
        state = .stopped
    }

    func openLogsFolder() {
        NSWorkspace.shared.activateFileViewerSelecting([logsDir])
    }

    func recoverIfBackendMissing() {
        DispatchQueue.global(qos: .utility).async { [weak self] in
            guard let self else { return }
            if self.isWebReady() {
                if self.state != .running {
                    self.appendLauncherLog("Recovery found web UI ready; marking running.\n")
                    self.state = .running
                }
                return
            }
            switch self.state {
            case .bootstrapping, .starting:
                break
            case .stopped, .running, .failed:
                return
            }

            guard self.beginLifecycleOperation() else {
                return
            }
            defer {
                self.endLifecycleOperation()
            }

            guard self.setupProcess == nil, self.process == nil else {
                return
            }
            switch self.state {
            case .bootstrapping, .starting:
                let python = self.venvDir.appendingPathComponent("bin/python")
                let profile = self.runtimeDir.appendingPathComponent("tater_profile.env")
                guard
                    FileManager.default.isExecutableFile(atPath: python.path),
                    FileManager.default.fileExists(atPath: profile.path)
                else {
                    return
                }
                self.appendLauncherLog("Recovery detected completed setup with no backend; launching backend.\n")
                do {
                    try self.launchBackend()
                } catch {
                    self.appendLauncherLog("Recovery launch failed: \(error.localizedDescription)\n")
                    self.state = .failed(error.localizedDescription)
                }
            case .stopped, .running, .failed:
                return
            }
        }
    }

    private func beginLifecycleOperation() -> Bool {
        lifecycleLock.lock()
        defer {
            lifecycleLock.unlock()
        }
        guard !lifecycleOperationInProgress else {
            return false
        }
        lifecycleOperationInProgress = true
        return true
    }

    private func endLifecycleOperation() {
        lifecycleLock.lock()
        lifecycleOperationInProgress = false
        lifecycleLock.unlock()
    }

    func recentLogText(maxBytes: Int = 180_000) -> String {
        let urls = [
            logsDir.appendingPathComponent("launcher.log"),
            logsDir.appendingPathComponent("setup.log"),
            logsDir.appendingPathComponent("tater.log")
        ]
        var chunks: [String] = []
        for url in urls where FileManager.default.fileExists(atPath: url.path) {
            if let text = tailText(from: url, maxBytes: maxBytes / max(1, urls.count)), !text.isEmpty {
                chunks.append("== \(url.lastPathComponent) ==\n\(text)")
            }
        }
        return chunks.joined(separator: "\n\n")
    }

    private func tailText(from url: URL, maxBytes: Int) -> String? {
        guard
            let handle = try? FileHandle(forReadingFrom: url),
            let size = try? handle.seekToEnd()
        else {
            return nil
        }
        let readSize = UInt64(max(1, maxBytes))
        let start = size > readSize ? size - readSize : 0
        do {
            try handle.seek(toOffset: start)
            let data = try handle.readToEnd() ?? Data()
            try handle.close()
            return String(decoding: data, as: UTF8.self)
        } catch {
            try? handle.close()
            return nil
        }
    }

    private static func resolveSourceRoot(supportRoot: URL) -> URL {
        let environment = ProcessInfo.processInfo.environment
        if let raw = environment["TATER_SOURCE_DIR"], !raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return URL(fileURLWithPath: NSString(string: raw).expandingTildeInPath, isDirectory: true)
        }

        if let bundledSource = Bundle.main.resourceURL?.appendingPathComponent("TaterSource", isDirectory: true),
           FileManager.default.fileExists(atPath: bundledSource.appendingPathComponent("tateros_app.py").path) {
            return bundledSource
        }

        let installedSource = supportRoot
            .appendingPathComponent("app", isDirectory: true)
            .appendingPathComponent("current", isDirectory: true)
        if FileManager.default.fileExists(atPath: installedSource.appendingPathComponent("tateros_app.py").path) {
            return installedSource
        }

        return URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
    }

    private func ensurePrivateFolders() throws {
        let folders = [
            supportRoot,
            appRoot,
            venvDir,
            pythonRoot,
            managedPythonDir,
            runtimeDir,
            agentRoot,
            logsDir,
            agentRoot.appendingPathComponent("workspace", isDirectory: true),
            agentRoot.appendingPathComponent("downloads", isDirectory: true),
            agentRoot.appendingPathComponent("documents", isDirectory: true),
            agentRoot.appendingPathComponent("artifacts", isDirectory: true),
            agentRoot.appendingPathComponent("models", isDirectory: true),
            agentRoot.appendingPathComponent("redis", isDirectory: true),
            agentRoot.appendingPathComponent("cores", isDirectory: true),
            agentRoot.appendingPathComponent("verba", isDirectory: true),
            agentRoot.appendingPathComponent("portals", isDirectory: true),
            agentRoot.appendingPathComponent("integrations", isDirectory: true)
        ]

        for folder in folders {
            try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
        }
    }

    private func validateDataRootAvailability() throws {
        let defaultRoot = supportRoot.appendingPathComponent("agent_lab", isDirectory: true).standardizedFileURL
        let selectedRoot = agentRoot.standardizedFileURL
        guard selectedRoot != defaultRoot else { return }

        let parent = selectedRoot.deletingLastPathComponent()
        guard FileManager.default.fileExists(atPath: parent.path) else {
            throw LauncherError(
                "Tater's data location is unavailable: \(selectedRoot.path). Reconnect the drive, then start Tater again or choose a new data location from the menu bar."
            )
        }
        if !FileManager.default.fileExists(atPath: selectedRoot.path) {
            throw LauncherError(
                "Tater's data folder is missing: \(selectedRoot.path). Tater will not create an empty replacement automatically."
            )
        }
    }

    private func ensurePythonRuntime() throws {
        let managedPython = managedPythonDir.appendingPathComponent("bin/python3.11").path
        if isUsablePython(managedPython) {
            selectedPythonPath = managedPython
            appendLog("Using managed Python: \(managedPython)\n")
            return
        }

        for candidate in systemPythonCandidates() {
            if isUsablePython(candidate) {
                selectedPythonPath = candidate
                appendLog("Using local Python: \(candidate)\n")
                return
            }
        }

        try installManagedPython()
    }

    private func systemPythonCandidates() -> [String] {
        [
            "/opt/homebrew/opt/python@3.11/bin/python3.11",
            "/opt/homebrew/bin/python3.11",
            "/usr/local/opt/python@3.11/bin/python3.11",
            "/usr/local/bin/python3.11",
            "/Library/Frameworks/Python.framework/Versions/3.11/bin/python3.11"
        ]
    }

    private func isUsablePython(_ path: String) -> Bool {
        guard FileManager.default.isExecutableFile(atPath: path) else {
            return false
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: path)
        process.arguments = ["-c", "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)"]
        process.standardOutput = Pipe()
        process.standardError = Pipe()
        do {
            try process.run()
            process.waitUntilExit()
            return process.terminationStatus == 0
        } catch {
            return false
        }
    }

    private func installManagedPython() throws {
        appendLog("No local Python 3.11 found. Installing managed Python under \(pythonRoot.path)\n")
        try FileManager.default.createDirectory(at: pythonRoot, withIntermediateDirectories: true)

        let assetURL = try findStandalonePythonAssetURL()
        appendLog("Downloading \(assetURL.absoluteString)\n")

        let archiveURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("tater-python-\(UUID().uuidString).tar.gz")
        let extractDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("tater-python-\(UUID().uuidString)", isDirectory: true)
        defer {
            try? FileManager.default.removeItem(at: archiveURL)
            try? FileManager.default.removeItem(at: extractDir)
        }

        try downloadFile(from: assetURL, to: archiveURL)
        try FileManager.default.createDirectory(at: extractDir, withIntermediateDirectories: true)
        try runCheckedProcess(
            executable: "/usr/bin/tar",
            arguments: ["-xzf", archiveURL.path, "-C", extractDir.path],
            currentDirectory: nil
        )

        let extractedRoot = try findExtractedPythonRoot(in: extractDir)
        if FileManager.default.fileExists(atPath: managedPythonDir.path) {
            try FileManager.default.removeItem(at: managedPythonDir)
        }
        try FileManager.default.moveItem(at: extractedRoot, to: managedPythonDir)

        let python = managedPythonDir.appendingPathComponent("bin/python3.11").path
        guard isUsablePython(python) else {
            throw LauncherError("Managed Python installed, but \(python) did not run as Python 3.11.")
        }
        selectedPythonPath = python
        appendLog("Managed Python ready: \(python)\n")
    }

    private func findStandalonePythonAssetURL() throws -> URL {
        let releasesURL = URL(string: "https://api.github.com/repos/astral-sh/python-build-standalone/releases?per_page=20")!
        var request = URLRequest(url: releasesURL)
        request.setValue("application/vnd.github+json", forHTTPHeaderField: "Accept")
        request.setValue("TaterAssistant", forHTTPHeaderField: "User-Agent")
        request.timeoutInterval = 60

        let data = try loadData(from: request)
        guard let releases = try JSONSerialization.jsonObject(with: data) as? [[String: Any]] else {
            throw LauncherError("Could not read python-build-standalone releases.")
        }

        let targetArch = standalonePythonArch()
        for release in releases {
            guard let assets = release["assets"] as? [[String: Any]] else {
                continue
            }
            for asset in assets {
                guard
                    let name = asset["name"] as? String,
                    let rawURL = asset["browser_download_url"] as? String,
                    name.hasPrefix("cpython-3.11."),
                    name.contains("-\(targetArch)-apple-darwin-install_only.tar.gz"),
                    !name.contains("stripped"),
                    let url = URL(string: rawURL)
                else {
                    continue
                }
                return url
            }
        }

        throw LauncherError("Could not find a standalone Python 3.11 build for \(targetArch)-apple-darwin.")
    }

    private func loadData(from request: URLRequest) throws -> Data {
        let semaphore = DispatchSemaphore(value: 0)
        var result: Result<Data, Error>?
        let task = URLSession.shared.dataTask(with: request) { data, response, error in
            if let error {
                result = .failure(error)
            } else if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
                result = .failure(LauncherError("Request failed with HTTP \(http.statusCode)."))
            } else {
                result = .success(data ?? Data())
            }
            semaphore.signal()
        }
        task.resume()
        semaphore.wait()

        guard let result else {
            throw LauncherError("Request did not complete.")
        }
        return try result.get()
    }

    private func standalonePythonArch() -> String {
        #if arch(arm64)
        return "aarch64"
        #else
        return "x86_64"
        #endif
    }

    private func downloadFile(from url: URL, to destination: URL) throws {
        let semaphore = DispatchSemaphore(value: 0)
        var result: Result<URL, Error>?
        let task = URLSession.shared.downloadTask(with: url) { location, _, error in
            if let error {
                result = .failure(error)
            } else if let location {
                result = .success(location)
            } else {
                result = .failure(LauncherError("Download finished without a file."))
            }
            semaphore.signal()
        }
        task.resume()
        semaphore.wait()

        guard let result else {
            throw LauncherError("Download did not complete.")
        }
        let tempURL = try result.get()
        if FileManager.default.fileExists(atPath: destination.path) {
            try FileManager.default.removeItem(at: destination)
        }
        try FileManager.default.copyItem(at: tempURL, to: destination)
    }

    private func findExtractedPythonRoot(in directory: URL) throws -> URL {
        guard let enumerator = FileManager.default.enumerator(
            at: directory,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            throw LauncherError("Could not inspect extracted Python archive.")
        }

        for case let url as URL in enumerator {
            if url.lastPathComponent == "python3.11",
               url.deletingLastPathComponent().lastPathComponent == "bin" {
                return url.deletingLastPathComponent().deletingLastPathComponent()
            }
        }
        throw LauncherError("Downloaded Python archive did not contain bin/python3.11.")
    }

    private func runCheckedProcess(executable: String, arguments: [String], currentDirectory: URL?) throws {
        appendLog("$ \(executable) \(arguments.joined(separator: " "))\n")
        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments
        process.currentDirectoryURL = currentDirectory

        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = pipe
        pipe.fileHandleForReading.readabilityHandler = { [weak self] reader in
            let data = reader.availableData
            guard !data.isEmpty else { return }
            self?.appendLog(String(decoding: data, as: UTF8.self))
        }

        try process.run()
        process.waitUntilExit()
        pipe.fileHandleForReading.readabilityHandler = nil
        guard process.terminationStatus == 0 else {
            throw LauncherError("\(executable) exited with status \(process.terminationStatus).")
        }
    }

    private func bootstrapIfNeeded() throws {
        let python = venvDir.appendingPathComponent("bin/python")
        let profile = runtimeDir.appendingPathComponent("tater_profile.env")
        let venvReady = FileManager.default.isExecutableFile(atPath: python.path)
        let profileReady = FileManager.default.fileExists(atPath: profile.path)
        let dependenciesReady = venvReady ? pinnedRuntimeDependenciesReady(using: python) : false
        let localLLMRuntimesReady = localLLMRuntimesReady()
        let airPlayRuntimeReady = venvReady ? airPlayDependenciesReady(using: python) : false
        guard !venvReady || !profileReady || !dependenciesReady || !localLLMRuntimesReady || !airPlayRuntimeReady else {
            appendLog("Private runtime is ready: \(venvDir.path)\n")
            return
        }
        appendLog("Private runtime needs setup; venvReady=\(venvReady) profileReady=\(profileReady) dependenciesReady=\(dependenciesReady) localLLMRuntimesReady=\(localLLMRuntimesReady) airPlayRuntimeReady=\(airPlayRuntimeReady). Running setup.\n")

        guard FileManager.default.fileExists(atPath: sourceRoot.appendingPathComponent("setup_tater.sh").path) else {
            throw LauncherError("Could not find setup_tater.sh in \(sourceRoot.path)")
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/bin/sh")
        process.arguments = ["setup_tater.sh", "macos"]
        process.currentDirectoryURL = sourceRoot
        process.environment = backendEnvironment()

        appendLog("Preparing private runtime in \(supportRoot.path)\n")
        appendLog("Running macOS setup from \(sourceRoot.path)\n")
        appendLauncherLog("Starting setup process.\n")

        let handle = try openLog(named: "setup.log", append: true)
        let pipe = streamProcessOutput(process, to: handle)

        setupProcess = process
        defer {
            setupProcess = nil
        }
        try process.run()
        process.waitUntilExit()
        pipe.fileHandleForReading.readabilityHandler = nil
        try? handle.close()
        appendLauncherLog("Setup process exited with status \(process.terminationStatus).\n")

        guard process.terminationStatus == 0 else {
            throw LauncherError("macOS setup failed. See \(logsDir.appendingPathComponent("setup.log").path)")
        }
        try recordRuntimeRequirementsFingerprint()
        appendLauncherLog("Recorded installed Python requirements fingerprint.\n")
    }

    private func localLLMRuntimesReady() -> Bool {
        let llamaReady = nativeLlamaServerReady()
        if !llamaReady {
            appendLog("Private runtime is missing native llama.cpp server under \(runtimeDir.appendingPathComponent("llama.cpp").path).\n")
        }
        return llamaReady
    }

    private func nativeLlamaServerReady() -> Bool {
        if let override = ProcessInfo.processInfo.environment["TATER_LLAMA_CPP_SERVER_BIN"],
           FileManager.default.isExecutableFile(atPath: NSString(string: override).expandingTildeInPath) {
            return true
        }
        if let bundled = bundledLlamaServerURL(), FileManager.default.isExecutableFile(atPath: bundled.path) {
            return true
        }

        let candidates = [
            runtimeDir.appendingPathComponent("llama.cpp/build/bin/llama-server"),
            runtimeDir.appendingPathComponent("llama.cpp/build/bin/Release/llama-server")
        ]
        return candidates.contains { FileManager.default.isExecutableFile(atPath: $0.path) }
    }

    private func bundledLlamaServerURL() -> URL? {
        guard let resources = Bundle.main.resourceURL else {
            return nil
        }
        return resources
            .appendingPathComponent("Native", isDirectory: true)
            .appendingPathComponent("llama.cpp", isDirectory: true)
            .appendingPathComponent("bin", isDirectory: true)
            .appendingPathComponent("llama-server")
    }

    private func bundledAirPlaySenderURL() -> URL? {
        Bundle.main.resourceURL?
            .appendingPathComponent("Native", isDirectory: true)
            .appendingPathComponent("airplay", isDirectory: true)
            .appendingPathComponent("bin", isDirectory: true)
            .appendingPathComponent("cliairplay")
    }

    private func bundledShairportSyncURL() -> URL? {
        Bundle.main.resourceURL?
            .appendingPathComponent("Native", isDirectory: true)
            .appendingPathComponent("airplay", isDirectory: true)
            .appendingPathComponent("bin", isDirectory: true)
            .appendingPathComponent("shairport-sync")
    }

    private func airPlayDependenciesReady(using python: URL) -> Bool {
        let environment = ProcessInfo.processInfo.environment
        let receiverOverride = environment["TATER_SHAIRPORT_SYNC_PATH"]
            .map { NSString(string: $0).expandingTildeInPath }
        let receiverCandidates = [
            receiverOverride.map { URL(fileURLWithPath: $0) },
            bundledShairportSyncURL(),
            runtimeDir
                .appendingPathComponent("external_audio", isDirectory: true)
                .appendingPathComponent("shairport-sync-v5.2.1", isDirectory: true)
                .appendingPathComponent("bin", isDirectory: true)
                .appendingPathComponent("shairport-sync")
        ].compactMap { $0 }
        let receiverReady = receiverCandidates.contains { candidate in
            guard FileManager.default.isExecutableFile(atPath: candidate.path) else {
                return false
            }
            guard let version = runProcessCombinedCapture(
                executable: candidate.path,
                arguments: ["-V"]
            ), version.hasPrefix("5.2.1-") else {
                return false
            }
            guard let help = runProcessCombinedCapture(
                executable: candidate.path,
                arguments: ["-h"]
            ) else {
                return false
            }
            return help.contains("--service-type") && help.contains("stdout")
        }
        if !receiverReady {
            appendLog("Private runtime is missing the pinned Shairport Sync AirPlay receiver.\n")
            return false
        }

        let senderOverride = environment["TATER_AIRPLAY_CLI_PATH"]
            .map { NSString(string: $0).expandingTildeInPath }
        let senderCandidates = [
            senderOverride.map { URL(fileURLWithPath: $0) },
            bundledAirPlaySenderURL(),
            runtimeDir
                .appendingPathComponent("airplay_bridge", isDirectory: true)
                .appendingPathComponent("0.4.12", isDirectory: true)
                .appendingPathComponent("cliairplay-macos-arm64")
        ].compactMap { $0 }
        let senderReady = senderCandidates.contains { candidate in
            FileManager.default.isExecutableFile(atPath: candidate.path)
                && runProcessCombinedCapture(
                    executable: candidate.path,
                    arguments: ["--check"]
                )?.localizedCaseInsensitiveContains("cliairplay") == true
        }
        if !senderReady {
            appendLog("Private runtime is missing the pinned AirPlay sender.\n")
            return false
        }

        let ffmpegCheck = Process()
        ffmpegCheck.executableURL = python
        ffmpegCheck.arguments = [
            "-c",
            "import os, subprocess, sys\nimport imageio_ffmpeg, zeroconf\npath=imageio_ffmpeg.get_ffmpeg_exe()\nok=os.path.isfile(path) and os.access(path, os.X_OK) and subprocess.run([path, '-version'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10).returncode == 0\nsys.exit(0 if ok else 1)"
        ]
        ffmpegCheck.standardOutput = FileHandle.nullDevice
        ffmpegCheck.standardError = FileHandle.nullDevice
        do {
            try ffmpegCheck.run()
            ffmpegCheck.waitUntilExit()
        } catch {
            appendLog("Private runtime could not check the media decoding runtime.\n")
            return false
        }
        if ffmpegCheck.terminationStatus != 0 {
            appendLog("Private runtime is missing its media decoder or AirPlay discovery dependency.\n")
            return false
        }
        return true
    }

    private var isAppleSilicon: Bool {
        #if arch(arm64)
        return true
        #else
        return false
        #endif
    }

    private func pinnedRuntimeDependenciesReady(using python: URL) -> Bool {
        guard let expected = sourceRequirementsFingerprint() else {
            appendLog("Private runtime could not fingerprint \(sourceRoot.appendingPathComponent("requirements.txt").path).\n")
            return false
        }

        let fingerprintURL = runtimeDir.appendingPathComponent("tater-requirements.sha256")
        guard
            let recorded = try? String(contentsOf: fingerprintURL, encoding: .utf8)
                .trimmingCharacters(in: .whitespacesAndNewlines),
            !recorded.isEmpty
        else {
            appendLog("Private runtime has no installed requirements fingerprint; setup will refresh dependencies.\n")
            return false
        }
        guard recorded == expected else {
            appendLog("Private runtime requirements changed; setup will refresh dependencies.\n")
            return false
        }
        guard installedPackageVersion("onnx-asr", using: python) != nil else {
            appendLog("Private runtime is missing the Parakeet ONNX dependency (onnx-asr).\n")
            return false
        }
        let obsoletePackages = [
            "aioesphomeapi",
            "mlx",
            "mlx-audio",
            "mlx-lm",
            "mlx-metal",
            "mlx-vlm",
            "mlx-whisper",
            "nanowakeword",
            "openwakeword",
            "outlines",
            "outlines-core",
            "scikit-learn",
            "narwhals",
            "threadpoolctl",
            "tzlocal",
            "tzdata"
        ]
        for packageName in obsoletePackages where installedPackageVersion(packageName, using: python) != nil {
            appendLog("Private runtime contains obsolete dependency \(packageName); setup will remove it.\n")
            return false
        }
        let legacyFaceIDEnvironments = [
            runtimeDir.appendingPathComponent("models/face-id/venv/pyvenv.cfg"),
            runtimeDir.deletingLastPathComponent().appendingPathComponent("models/face-id/venv/pyvenv.cfg")
        ]
        if legacyFaceIDEnvironments.contains(where: { FileManager.default.fileExists(atPath: $0.path) }) {
            appendLog("Private runtime contains an obsolete Face ID worker environment; setup will remove it.\n")
            return false
        }
        let obsoleteRuntimeDirectories = [
            runtimeDir.appendingPathComponent("mlx-engine", isDirectory: true),
            agentRoot.appendingPathComponent("models/tts/chatterbox-local/runtime", isDirectory: true)
        ]
        if obsoleteRuntimeDirectories.contains(where: { FileManager.default.fileExists(atPath: $0.path) }) {
            appendLog("Private runtime contains a retired model engine environment; setup will remove it.\n")
            return false
        }
        let legacyFirmwareRoot = agentRoot.appendingPathComponent("esphome", isDirectory: true)
        let firmwareRoot = agentRoot.appendingPathComponent("firmware", isDirectory: true)
        if FileManager.default.fileExists(atPath: legacyFirmwareRoot.path),
           !FileManager.default.fileExists(atPath: firmwareRoot.path) {
            appendLog("Private runtime uses the legacy firmware workspace name; setup will rename it.\n")
            return false
        }
        var faceIDPackages = ["deepface", "tensorflow", "tf-keras", "opencv-python", "retina-face"]
        if isAppleSilicon {
            faceIDPackages.append("tensorflow-metal")
        }
        for packageName in faceIDPackages where installedPackageVersion(packageName, using: python) == nil {
            appendLog("Private runtime is missing the Face ID dependency \(packageName).\n")
            return false
        }
        return true
    }

    private func sourceRequirementsFingerprint() -> String? {
        let requirementsURL = sourceRoot.appendingPathComponent("requirements.txt")
        guard let data = try? Data(contentsOf: requirementsURL) else {
            return nil
        }
        return SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    private func recordRuntimeRequirementsFingerprint() throws {
        guard let fingerprint = sourceRequirementsFingerprint() else {
            throw LauncherError("Could not fingerprint the installed Python requirements.")
        }
        let fingerprintURL = runtimeDir.appendingPathComponent("tater-requirements.sha256")
        try "\(fingerprint)\n".write(to: fingerprintURL, atomically: true, encoding: .utf8)
    }

    private func pinnedRequirementVersion(for packageName: String) -> String? {
        let requirements = sourceRoot.appendingPathComponent("requirements.txt")
        guard let contents = try? String(contentsOf: requirements) else {
            return nil
        }

        let prefix = "\(packageName)=="
        for rawLine in contents.components(separatedBy: .newlines) {
            let uncommented = rawLine.split(separator: "#", maxSplits: 1, omittingEmptySubsequences: false).first.map(String.init) ?? rawLine
            let line = uncommented.trimmingCharacters(in: .whitespacesAndNewlines)
            guard line.range(of: prefix, options: [.anchored, .caseInsensitive]) != nil else {
                continue
            }
            return String(line.dropFirst(prefix.count)).trimmingCharacters(in: .whitespacesAndNewlines)
        }
        return nil
    }

    private func installedPackageVersion(_ packageName: String, using python: URL) -> String? {
        let process = Process()
        process.executableURL = python
        process.arguments = [
            "-c",
            "from importlib.metadata import PackageNotFoundError, version\nimport sys\ntry:\n    print(version(sys.argv[1]))\nexcept PackageNotFoundError:\n    sys.exit(2)",
            packageName
        ]

        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = Pipe()

        do {
            try process.run()
            process.waitUntilExit()
        } catch {
            return nil
        }

        guard process.terminationStatus == 0 else {
            return nil
        }
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        let output = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
        return output.isEmpty ? nil : output
    }

    private func launchBackend() throws {
        state = .starting

        guard FileManager.default.fileExists(atPath: sourceRoot.appendingPathComponent("run_ui.sh").path) else {
            throw LauncherError("Could not find run_ui.sh in \(sourceRoot.path)")
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/bin/sh")
        process.arguments = ["run_ui.sh"]
        process.currentDirectoryURL = sourceRoot
        process.environment = backendEnvironment()

        appendLog("Starting Tater backend on \(webURL.absoluteString)\n")
        appendLauncherLog("Opening backend log and starting run_ui.sh.\n")

        let handle = try openLog(named: "tater.log", append: true)
        logHandle = handle
        outputPipe = streamProcessOutput(process, to: handle)
        process.terminationHandler = { [weak self] proc in
            DispatchQueue.main.async {
                guard let self else { return }
                self.detachOutputPipe()
                self.closeLogHandle()
                self.process = nil
                if proc.terminationStatus == 0 {
                    self.state = .stopped
                } else {
                    self.state = .failed("Backend exited with status \(proc.terminationStatus)")
                }
            }
        }

        try process.run()
        self.process = process
        appendLauncherLog("Backend process started with pid \(process.processIdentifier).\n")

        if waitForWebReady(timeout: 60) {
            appendLauncherLog("Backend web UI is ready.\n")
            state = .running
        } else if process.isRunning {
            appendLauncherLog("Backend is still starting after readiness timeout.\n")
            state = .starting
        } else {
            throw LauncherError("Backend exited before the web UI became ready.")
        }
    }

    private func backendEnvironment() -> [String: String] {
        var environment = ProcessInfo.processInfo.environment
        let profileEnv = runtimeDir.appendingPathComponent("tater_profile.env")
        let llmRoot = agentRoot.appendingPathComponent("models/llm", isDirectory: true)
        let pathPrefix = [
            "/opt/homebrew/opt/python@3.11/bin",
            "/opt/homebrew/bin",
            "/usr/local/opt/python@3.11/bin",
            "/usr/local/bin",
            "/usr/bin",
            "/bin"
        ]
        let existingPath = environment["PATH"] ?? ""
        let preferredPython = "/opt/homebrew/opt/python@3.11/bin/python3.11"

        environment["PATH"] = (pathPrefix + [existingPath]).filter { !$0.isEmpty }.joined(separator: ":")
        if let selectedPythonPath {
            environment["PYTHON"] = selectedPythonPath
        } else if FileManager.default.isExecutableFile(atPath: preferredPython) {
            environment["PYTHON"] = preferredPython
        }
        environment["PYTHONUNBUFFERED"] = "1"
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        environment["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
        environment["TATER_SETUP_REBUILD"] = "1"
        environment["TATER_SETUP_REQUIRE_LOCAL_LLM"] = "1"
        environment["TATER_EARLY_WEB_UI_ON_STARTUP"] = "1"
        environment["TATER_VENV_DIR"] = venvDir.path
        environment["TATER_RUNTIME_DIR"] = runtimeDir.path
        environment["TATER_PROFILE_ENV"] = profileEnv.path
        environment["TATER_AGENT_ROOT"] = agentRoot.path
        environment["TATER_CORE_DIR"] = agentRoot.appendingPathComponent("cores", isDirectory: true).path
        environment["TATER_VERBA_DIR"] = agentRoot.appendingPathComponent("verba", isDirectory: true).path
        environment["TATER_PORTAL_DIR"] = agentRoot.appendingPathComponent("portals", isDirectory: true).path
        environment["TATER_INTEGRATION_DIR"] = agentRoot.appendingPathComponent("integrations", isDirectory: true).path
        environment["TATER_REDIS_CONFIG_PATH"] = runtimeDir.appendingPathComponent("redis_connection.json").path
        environment["TATER_REDIS_DATA_PATH"] = agentRoot.appendingPathComponent("redis/dump.rdb").path
        environment["TATER_LOCAL_LLM_MODEL_REGISTRY"] = llmRoot.appendingPathComponent("downloaded_models.json").path
        if let resources = Bundle.main.resourceURL {
            environment["TATER_APP_RESOURCES_DIR"] = resources.path
        }
        environment["TATER_LLAMA_CPP_MODEL_ROOT"] = llmRoot.appendingPathComponent("llama-cpp", isDirectory: true).path
        if environment["TATER_LLAMA_CPP_SERVER_BIN"]?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ?? true,
           let bundled = bundledLlamaServerURL(),
           FileManager.default.isExecutableFile(atPath: bundled.path) {
            environment["TATER_LLAMA_CPP_SERVER_BIN"] = bundled.path
        }
        if let bundled = bundledAirPlaySenderURL(),
           FileManager.default.isExecutableFile(atPath: bundled.path) {
            environment["TATER_AIRPLAY_CLI_PATH"] = bundled.path
        }
        if let bundled = bundledShairportSyncURL(),
           FileManager.default.isExecutableFile(atPath: bundled.path) {
            environment["TATER_SHAIRPORT_SYNC_PATH"] = bundled.path
        }
        environment["HF_HOME"] = agentRoot.appendingPathComponent("models/huggingface-cache", isDirectory: true).path
        environment["TORCH_HOME"] = agentRoot.appendingPathComponent("models/torch-cache", isDirectory: true).path
        environment["HTMLUI_HOST"] = "0.0.0.0"
        environment["HTMLUI_PORT"] = "\(taterPort)"
        environment["TATER_PYTHON"] = venvDir.appendingPathComponent("bin/python").path
        environment["TATER_NATIVE_STATUS_TOKEN"] = nativeStatusToken
        return environment
    }

    private func openLog(named name: String, append: Bool) throws -> FileHandle {
        try FileManager.default.createDirectory(at: logsDir, withIntermediateDirectories: true)
        let url = logsDir.appendingPathComponent(name)
        if !FileManager.default.fileExists(atPath: url.path) {
            FileManager.default.createFile(atPath: url.path, contents: nil)
        }
        let handle = try FileHandle(forWritingTo: url)
        if append {
            try handle.seekToEnd()
            let header = "\n\n=== \(Date()) ===\n".data(using: .utf8) ?? Data()
            try handle.write(contentsOf: header)
        } else {
            try handle.truncate(atOffset: 0)
        }
        return handle
    }

    private func streamProcessOutput(_ process: Process, to handle: FileHandle) -> Pipe {
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = pipe
        pipe.fileHandleForReading.readabilityHandler = { [weak self] reader in
            let data = reader.availableData
            guard !data.isEmpty else {
                reader.readabilityHandler = nil
                return
            }
            try? handle.write(contentsOf: data)
            if let text = String(data: data, encoding: .utf8) {
                self?.appendLog(text)
            } else {
                self?.appendLog(String(decoding: data, as: UTF8.self))
            }
        }
        return pipe
    }

    private func detachOutputPipe() {
        outputPipe?.fileHandleForReading.readabilityHandler = nil
        outputPipe = nil
    }

    private func appendLog(_ text: String) {
        guard !text.isEmpty else { return }
        DispatchQueue.main.async { [onLogAppend] in
            onLogAppend?(text)
        }
    }

    private func appendLauncherLog(_ text: String) {
        guard !text.isEmpty else { return }
        do {
            try FileManager.default.createDirectory(at: logsDir, withIntermediateDirectories: true)
            let url = logsDir.appendingPathComponent("launcher.log")
            if !FileManager.default.fileExists(atPath: url.path) {
                FileManager.default.createFile(atPath: url.path, contents: nil)
            }
            let handle = try FileHandle(forWritingTo: url)
            try handle.seekToEnd()
            let stamp = ISO8601DateFormatter().string(from: Date())
            let data = "[\(stamp)] \(text)".data(using: .utf8) ?? Data()
            try handle.write(contentsOf: data)
            try handle.close()
        } catch {
            appendLog("Launcher log write failed: \(error.localizedDescription)\n")
        }
    }

    private func closeLogHandle() {
        try? logHandle?.close()
        logHandle = nil
    }

    private func isManagedProcessRunning() -> Bool {
        if let process, process.isRunning {
            return true
        }
        if let pid = externalBackendPID, isTaterBackendProcess(pid) {
            return true
        }
        if let pid = backendProcessPIDOnPort() {
            externalBackendPID = pid
            return true
        }
        externalBackendPID = nil
        return false
    }

    private func waitForManagedProcessExit(
        _ process: Process,
        descendants: [pid_t],
        timeout: TimeInterval
    ) {
        let deadline = Date().addingTimeInterval(timeout)
        while process.isRunning && Date() < deadline {
            Thread.sleep(forTimeInterval: 0.1)
        }
        if process.isRunning {
            appendLauncherLog("Managed backend pid \(process.processIdentifier) did not exit; sending SIGKILL.\n")
            Darwin.kill(process.processIdentifier, SIGKILL)
        }
        detachOutputPipe()
        process.waitUntilExit()
        terminateCapturedDescendants(descendants, naturalExitGrace: descendantExitGrace)
    }

    private func terminateBackendProcess(pid: pid_t, waitForExit: Bool) {
        if waitForExit {
            terminateBackendProcessNow(pid: pid)
        } else {
            DispatchQueue.global(qos: .utility).async { [weak self] in
                self?.terminateBackendProcessNow(pid: pid)
            }
        }
    }

    private func terminateBackendProcessNow(pid: pid_t) {
        let descendants = descendantProcessIDs(of: pid)
        if processExists(pid) {
            Darwin.kill(pid, SIGTERM)
        }
        if !waitForProcessExit(pid, timeout: gracefulStopTimeout) {
            appendLauncherLog("Discovered backend pid \(pid) did not exit; sending SIGKILL.\n")
            Darwin.kill(pid, SIGKILL)
            _ = waitForProcessExit(pid, timeout: 2)
        }
        terminateCapturedDescendants(descendants, naturalExitGrace: descendantExitGrace)
    }

    private func terminateCapturedDescendants(
        _ pids: [pid_t],
        naturalExitGrace: TimeInterval = 0
    ) {
        var remaining = pids.filter { processExists($0) }
        if !remaining.isEmpty && naturalExitGrace > 0 {
            let deadline = Date().addingTimeInterval(naturalExitGrace)
            while !remaining.isEmpty && Date() < deadline {
                Thread.sleep(forTimeInterval: 0.1)
                remaining = remaining.filter { processExists($0) }
            }
        }
        guard !remaining.isEmpty else { return }
        appendLauncherLog(
            "Cleaning up \(remaining.count) backend descendant process(es): "
                + remaining.map(String.init).joined(separator: ", ")
                + ".\n"
        )
        for pid in remaining {
            Darwin.kill(pid, SIGTERM)
        }
        let termDeadline = Date().addingTimeInterval(2)
        while remaining.contains(where: processExists) && Date() < termDeadline {
            Thread.sleep(forTimeInterval: 0.1)
        }
        let stubborn = remaining.filter { processExists($0) }
        for pid in stubborn {
            Darwin.kill(pid, SIGKILL)
        }
        if !stubborn.isEmpty {
            appendLauncherLog(
                "Forced \(stubborn.count) backend descendant process(es) to exit: "
                    + stubborn.map(String.init).joined(separator: ", ")
                    + ".\n"
            )
        }
    }

    private func cleanupStaleBundledLlamaServers() {
        guard let serverPath = bundledLlamaServerURL()?.standardizedFileURL.path,
              let output = runProcessCapture(
                  executable: "/bin/ps",
                  arguments: ["-ww", "-axo", "pid=,command="]
              ) else {
            return
        }
        var stale: [pid_t] = []
        for line in output.split(whereSeparator: \.isNewline) {
            let parts = line.split(maxSplits: 1, whereSeparator: \.isWhitespace)
            guard parts.count == 2,
                  let pid = pid_t(parts[0]),
                  pid > 0,
                  pid != getpid() else {
                continue
            }
            let command = String(parts[1])
            let managedAlias = command.contains("--alias tater-llama")
                || command.contains("--alias=tater-llama")
                || command.contains("--alias qwen3-asr")
                || command.contains("--alias=qwen3-asr")
            let ownsBinary = command == serverPath
                || command.hasPrefix(serverPath + " ")
                || command.hasPrefix("\"" + serverPath + "\" ")
            if managedAlias && ownsBinary {
                stale.append(pid)
            }
        }
        guard !stale.isEmpty else { return }
        appendLauncherLog(
            "Cleaning up \(stale.count) stale bundled llama-server process(es): "
                + stale.map(String.init).joined(separator: ", ")
                + ".\n"
        )
        for pid in stale {
            Darwin.kill(pid, SIGKILL)
        }
        let deadline = Date().addingTimeInterval(2)
        while stale.contains(where: processExists) && Date() < deadline {
            Thread.sleep(forTimeInterval: 0.05)
        }
        let remaining = stale.filter(processExists)
        if !remaining.isEmpty {
            appendLauncherLog(
                "Bundled llama-server process(es) still present after cleanup: "
                    + remaining.map(String.init).joined(separator: ", ")
                    + ".\n"
            )
        }
    }

    private func waitForProcessExit(_ pid: pid_t, timeout: TimeInterval) -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while processExists(pid) && Date() < deadline {
            Thread.sleep(forTimeInterval: 0.1)
        }
        return !processExists(pid)
    }

    private func processExists(_ pid: pid_t) -> Bool {
        guard pid > 0 else { return false }
        if Darwin.kill(pid, 0) == 0 {
            return true
        }
        return errno == EPERM
    }

    private func backendProcessPIDOnPort() -> pid_t? {
        guard let output = runProcessCapture(
            executable: "/usr/sbin/lsof",
            arguments: ["-nP", "-tiTCP:\(taterPort)", "-sTCP:LISTEN"]
        ) else {
            return nil
        }
        for line in output.split(whereSeparator: \.isNewline) {
            guard let pid = pid_t(String(line).trimmingCharacters(in: .whitespacesAndNewlines)),
                  pid != getpid(),
                  isTaterBackendProcess(pid) else {
                continue
            }
            return pid
        }
        return nil
    }

    private func isTaterBackendProcess(_ pid: pid_t) -> Bool {
        guard processExists(pid),
              let command = processCommandLine(pid) else {
            return false
        }
        return command.contains("uvicorn")
            && command.contains("tateros_app:app")
            && command.contains("--port \(taterPort)")
    }

    private func processCommandLine(_ pid: pid_t) -> String? {
        runProcessCapture(executable: "/bin/ps", arguments: ["-p", "\(pid)", "-ww", "-o", "command="])
    }

    private func descendantProcessIDs(of rootPID: pid_t) -> [pid_t] {
        guard let output = runProcessCapture(executable: "/bin/ps", arguments: ["-axo", "pid=,ppid="]) else {
            return []
        }
        var childrenByParent: [pid_t: [pid_t]] = [:]
        for line in output.split(whereSeparator: \.isNewline) {
            let parts = line.split(whereSeparator: \.isWhitespace)
            guard parts.count >= 2,
                  let pid = pid_t(parts[0]),
                  let parent = pid_t(parts[1]),
                  pid != getpid() else {
                continue
            }
            childrenByParent[parent, default: []].append(pid)
        }

        var descendants: [pid_t] = []
        var stack = childrenByParent[rootPID] ?? []
        while let pid = stack.popLast() {
            descendants.append(pid)
            stack.append(contentsOf: childrenByParent[pid] ?? [])
        }
        return descendants
    }

    private func runProcessCapture(executable: String, arguments: [String]) -> String? {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = FileHandle.nullDevice
        let data: Data
        do {
            try process.run()
            data = pipe.fileHandleForReading.readDataToEndOfFile()
            process.waitUntilExit()
        } catch {
            return nil
        }
        guard process.terminationStatus == 0 else {
            return nil
        }
        let output = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
        return output.isEmpty ? nil : output
    }

    private func runProcessCombinedCapture(executable: String, arguments: [String]) -> String? {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = pipe
        let data: Data
        do {
            try process.run()
            data = pipe.fileHandleForReading.readDataToEndOfFile()
            process.waitUntilExit()
        } catch {
            return nil
        }
        guard process.terminationStatus == 0 else {
            return nil
        }
        return String(decoding: data, as: UTF8.self)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func waitForWebReady(timeout: TimeInterval) -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if isWebReady() {
                return true
            }
            Thread.sleep(forTimeInterval: 0.75)
        }
        return false
    }

    private func isWebReady() -> Bool {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/curl")
        process.arguments = [
            "--fail",
            "--silent",
            "--show-error",
            "--max-time",
            "1.5",
            "--output",
            "/dev/null",
            webURL.absoluteString
        ]
        process.standardOutput = Pipe()
        process.standardError = Pipe()
        do {
            try process.run()
            process.waitUntilExit()
            return process.terminationStatus == 0
        } catch {
            return false
        }
    }
}

private struct UpdateManifest: Decodable, Equatable {
    let version: String
    let build: Int
    let url: URL
    let sha256: String
    let notes: String?
}

private enum UpdateState: Equatable {
    case idle
    case checking
    case current
    case available(UpdateManifest)
    case downloading(UpdateManifest)
    case installing(UpdateManifest)
    case failed(String)

    var isBusy: Bool {
        switch self {
        case .checking, .downloading, .installing:
            return true
        case .idle, .current, .available, .failed:
            return false
        }
    }
}

private final class UpdateManager {
    var onStateChange: ((UpdateState) -> Void)?

    private let updatesRoot: URL
    private var availableManifest: UpdateManifest?

    private(set) var state: UpdateState = .idle {
        didSet {
            DispatchQueue.main.async { [state, onStateChange] in
                onStateChange?(state)
            }
        }
    }

    init() {
        updatesRoot = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".taterassistant", isDirectory: true)
            .appendingPathComponent("updates", isDirectory: true)
    }

    func checkForUpdates(manual: Bool) {
        guard !state.isBusy else { return }
        state = .checking

        DispatchQueue.global(qos: .utility).async { [weak self] in
            guard let self else { return }
            do {
                let manifest = try self.fetchManifest()
                if self.isNewerThanCurrent(manifest) {
                    self.availableManifest = manifest
                    self.state = .available(manifest)
                } else {
                    self.availableManifest = nil
                    self.state = manual ? .current : .idle
                }
            } catch {
                self.state = manual ? .failed(error.localizedDescription) : .idle
            }
        }
    }

    func installAvailableUpdate() {
        let manifest: UpdateManifest?
        switch state {
        case .available(let current):
            manifest = current
        case .failed, .current, .idle, .checking, .downloading, .installing:
            manifest = availableManifest
        }

        guard let manifest else { return }
        state = .downloading(manifest)

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self else { return }
            do {
                let newApp = try self.prepareUpdate(manifest)
                self.state = .installing(manifest)
                try self.launchInstaller(newApp: newApp)
                DispatchQueue.main.async {
                    NSApp.terminate(nil)
                }
            } catch {
                self.state = .failed(error.localizedDescription)
            }
        }
    }

    private func fetchManifest() throws -> UpdateManifest {
        guard let url = manifestURL() else {
            throw LauncherError("No update manifest URL is configured.")
        }

        var request = URLRequest(url: url)
        request.cachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        request.timeoutInterval = 30
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.setValue("Tater", forHTTPHeaderField: "User-Agent")

        let data = try loadData(from: request)
        return try JSONDecoder().decode(UpdateManifest.self, from: data)
    }

    private func manifestURL() -> URL? {
        let environment = ProcessInfo.processInfo.environment
        for key in ["TATER_UPDATE_MANIFEST_URL", "TATER_ASSISTANT_UPDATE_MANIFEST_URL"] {
            if let raw = environment[key]?.trimmingCharacters(in: .whitespacesAndNewlines),
               !raw.isEmpty,
               let url = URL(string: raw) {
                return url
            }
        }

        if let raw = Bundle.main.object(forInfoDictionaryKey: "TaterUpdateManifestURL") as? String {
            return URL(string: raw.trimmingCharacters(in: .whitespacesAndNewlines))
        }
        return nil
    }

    private func isNewerThanCurrent(_ manifest: UpdateManifest) -> Bool {
        let currentVersion = Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String ?? "0"
        let versionComparison = compareVersion(manifest.version, to: currentVersion)
        if versionComparison != .orderedSame {
            return versionComparison == .orderedDescending
        }

        let currentBuild = buildNumber(from: Bundle.main.object(forInfoDictionaryKey: "CFBundleVersion") as? String)
        return manifest.build > currentBuild
    }

    private func compareVersion(_ lhs: String, to rhs: String) -> ComparisonResult {
        let left = versionParts(lhs)
        let right = versionParts(rhs)
        for index in 0..<max(left.count, right.count) {
            let leftPart = index < left.count ? left[index] : 0
            let rightPart = index < right.count ? right[index] : 0
            if leftPart > rightPart {
                return .orderedDescending
            }
            if leftPart < rightPart {
                return .orderedAscending
            }
        }
        return .orderedSame
    }

    private func versionParts(_ raw: String) -> [Int] {
        raw.split { !$0.isNumber }.compactMap { Int($0) }
    }

    private func buildNumber(from raw: String?) -> Int {
        let value = raw ?? "0"
        return versionParts(value).first ?? 0
    }

    private func prepareUpdate(_ manifest: UpdateManifest) throws -> URL {
        try FileManager.default.createDirectory(at: updatesRoot, withIntermediateDirectories: true)

        let archiveName = "Tater-\(safePathComponent(versionLabel(manifest.version))).zip"
        let archiveURL = updatesRoot.appendingPathComponent(archiveName)
        let extractDir = updatesRoot.appendingPathComponent("staging-\(UUID().uuidString)", isDirectory: true)

        try downloadFile(from: manifest.url, to: archiveURL)
        try verifySHA256(of: archiveURL, expected: manifest.sha256)
        try FileManager.default.createDirectory(at: extractDir, withIntermediateDirectories: true)
        try runCheckedProcess(
            executable: "/usr/bin/ditto",
            arguments: ["-x", "-k", archiveURL.path, extractDir.path]
        )

        let newApp = try findExtractedApp(in: extractDir)
        guard FileManager.default.fileExists(
            atPath: newApp.appendingPathComponent("Contents/MacOS/TaterAssistant").path
        ) else {
            throw LauncherError("Downloaded update did not contain the Tater executable.")
        }
        return newApp
    }

    private func verifySHA256(of url: URL, expected rawExpected: String) throws {
        let expected = rawExpected.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let hex = CharacterSet(charactersIn: "0123456789abcdef")
        guard expected.count == 64, expected.unicodeScalars.allSatisfy({ hex.contains($0) }) else {
            throw LauncherError("Update manifest is missing a valid SHA-256 hash.")
        }

        let actual = try sha256Hex(of: url)
        guard actual == expected else {
            throw LauncherError("Downloaded update did not match the manifest SHA-256.")
        }
    }

    private func sha256Hex(of url: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: url)
        defer {
            try? handle.close()
        }

        var hasher = SHA256()
        while true {
            let chunk = try handle.read(upToCount: 1024 * 1024) ?? Data()
            if chunk.isEmpty {
                break
            }
            hasher.update(data: chunk)
        }
        return hasher.finalize().map { String(format: "%02x", $0) }.joined()
    }

    private func findExtractedApp(in directory: URL) throws -> URL {
        let preferred = directory.appendingPathComponent("Tater.app", isDirectory: true)
        if FileManager.default.fileExists(atPath: preferred.path) {
            return preferred
        }

        guard let enumerator = FileManager.default.enumerator(
            at: directory,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        ) else {
            throw LauncherError("Could not inspect extracted update.")
        }

        for case let url as URL in enumerator where url.pathExtension == "app" {
            return url
        }
        throw LauncherError("Downloaded update did not contain a macOS app.")
    }

    private func launchInstaller(newApp: URL) throws {
        let targetApp = try currentAppURL()
        let scriptURL = try writeInstallerScript()

        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/bin/sh")
        process.arguments = [scriptURL.path, "\(getpid())", newApp.path, targetApp.path]
        try process.run()
    }

    private func currentAppURL() throws -> URL {
        let bundleURL = Bundle.main.bundleURL.standardizedFileURL
        guard bundleURL.pathExtension == "app" else {
            throw LauncherError("Tater is not running from an app bundle.")
        }
        return bundleURL
    }

    private func writeInstallerScript() throws -> URL {
        try FileManager.default.createDirectory(at: updatesRoot, withIntermediateDirectories: true)
        let scriptURL = updatesRoot.appendingPathComponent("install-update-\(UUID().uuidString).sh")
        let script = """
        #!/bin/sh
        set -eu

        APP_PID="$1"
        NEW_APP="$2"
        TARGET_APP="$3"
        SCRIPT_PATH="$0"
        WAIT_COUNT=0

        while kill -0 "$APP_PID" 2>/dev/null && [ "$WAIT_COUNT" -lt 225 ]; do
          sleep 0.2
          WAIT_COUNT=$((WAIT_COUNT + 1))
        done

        if kill -0 "$APP_PID" 2>/dev/null; then
          kill -TERM "$APP_PID" 2>/dev/null || true
          WAIT_COUNT=0
          while kill -0 "$APP_PID" 2>/dev/null && [ "$WAIT_COUNT" -lt 25 ]; do
            sleep 0.2
            WAIT_COUNT=$((WAIT_COUNT + 1))
          done
        fi

        if kill -0 "$APP_PID" 2>/dev/null; then
          kill -KILL "$APP_PID" 2>/dev/null || true
          WAIT_COUNT=0
          while kill -0 "$APP_PID" 2>/dev/null && [ "$WAIT_COUNT" -lt 25 ]; do
            sleep 0.2
            WAIT_COUNT=$((WAIT_COUNT + 1))
          done
        fi

        if kill -0 "$APP_PID" 2>/dev/null; then
          exit 1
        fi

        TARGET_PARENT="$(dirname "$TARGET_APP")"
        TARGET_NAME="$(basename "$TARGET_APP")"
        STAGED="${TARGET_PARENT}/.${TARGET_NAME}.updating"
        BACKUP="${TARGET_PARENT}/.${TARGET_NAME}.previous"
        NEW_PARENT="$(dirname "$NEW_APP")"

        rm -rf "$STAGED"
        ditto "$NEW_APP" "$STAGED"
        rm -rf "$BACKUP"
        if [ -d "$TARGET_APP" ]; then
          mv "$TARGET_APP" "$BACKUP"
        fi
        mv "$STAGED" "$TARGET_APP"
        xattr -dr com.apple.quarantine "$TARGET_APP" 2>/dev/null || true
        open "$TARGET_APP"
        rm -rf "$NEW_PARENT"
        rm -f "$SCRIPT_PATH"
        """

        try script.write(to: scriptURL, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: scriptURL.path)
        return scriptURL
    }

    private func safePathComponent(_ raw: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "._-"))
        let value = raw.unicodeScalars.map { allowed.contains($0) ? String($0) : "-" }.joined()
        return value.isEmpty ? "update" : value
    }

    private func versionLabel(_ raw: String) -> String {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.lowercased().hasPrefix("v") {
            return trimmed
        }
        return "v\(trimmed)"
    }

    private func loadData(from request: URLRequest) throws -> Data {
        let semaphore = DispatchSemaphore(value: 0)
        var result: Result<Data, Error>?
        let task = URLSession.shared.dataTask(with: request) { data, response, error in
            if let error {
                result = .failure(error)
            } else if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
                result = .failure(LauncherError("Update request failed with HTTP \(http.statusCode)."))
            } else {
                result = .success(data ?? Data())
            }
            semaphore.signal()
        }
        task.resume()
        semaphore.wait()

        guard let result else {
            throw LauncherError("Update request did not complete.")
        }
        return try result.get()
    }

    private func downloadFile(from url: URL, to destination: URL) throws {
        let semaphore = DispatchSemaphore(value: 0)
        var result: Result<URL, Error>?
        let task = URLSession.shared.downloadTask(with: url) { location, response, error in
            if let error {
                result = .failure(error)
            } else if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
                result = .failure(LauncherError("Update download failed with HTTP \(http.statusCode)."))
            } else if let location {
                result = .success(location)
            } else {
                result = .failure(LauncherError("Update download finished without a file."))
            }
            semaphore.signal()
        }
        task.resume()
        semaphore.wait()

        guard let result else {
            throw LauncherError("Update download did not complete.")
        }

        let tempURL = try result.get()
        if FileManager.default.fileExists(atPath: destination.path) {
            try FileManager.default.removeItem(at: destination)
        }
        try FileManager.default.copyItem(at: tempURL, to: destination)
    }

    private func runCheckedProcess(executable: String, arguments: [String]) throws {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments
        process.standardOutput = Pipe()
        process.standardError = Pipe()
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            throw LauncherError("\(executable) exited with status \(process.terminationStatus).")
        }
    }
}

private enum TaterMascotMood: Equatable {
    case idle
    case starting
    case running
    case tool
    case attention
    case stopped

    static var themeOrange: NSColor {
        NSColor(calibratedRed: 1.0, green: 0.48, blue: 0.04, alpha: 1.0)
    }

    var glowColor: NSColor {
        Self.themeOrange.withAlphaComponent(0.30)
    }

    var accentColor: NSColor {
        Self.themeOrange
    }
}

private struct TaterMascotDisplay: Equatable {
    let mood: TaterMascotMood
    let message: String
    let detail: String
}

private struct NativeMascotStatus: Decodable {
    let state: String?
    let message: String?
    let detail: String?
    let activeCount: Int?

    enum CodingKeys: String, CodingKey {
        case state
        case message
        case detail
        case activeCount = "active_count"
    }
}

private final class TaterMascotBubbleView: NSView {
    var display = TaterMascotDisplay(mood: .idle, message: "Tater is idle", detail: "Ready when you are.") {
        didSet {
            if oldValue != display {
                needsDisplay = true
            }
        }
    }
    var phase: CGFloat = 0 {
        didSet { needsDisplay = true }
    }

    override var isOpaque: Bool {
        false
    }

    func preferredSize(maxWidth: CGFloat) -> NSSize {
        let safeMaxWidth = max(132, min(maxWidth, 232))
        let message = display.message.trimmingCharacters(in: .whitespacesAndNewlines)
        let detail = display.detail.trimmingCharacters(in: .whitespacesAndNewlines)
        let longestText = max(message.count, detail.count)
        let targetWidth = min(safeMaxWidth, max(136, 112 + CGFloat(min(longestText, 84)) * 1.45))
        let textWidth = max(90, targetWidth - 32)

        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .center
        paragraph.lineBreakMode = .byWordWrapping
        let titleAttributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: 11, weight: .semibold),
            .paragraphStyle: paragraph
        ]
        let detailAttributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: 9, weight: .regular),
            .paragraphStyle: paragraph
        ]

        let titleBounds = NSString(string: message.isEmpty ? "Tater is thinking" : message).boundingRect(
            with: NSSize(width: textWidth, height: 40),
            options: [.usesLineFragmentOrigin],
            attributes: titleAttributes
        )
        let titleHeight = min(34, max(14, ceil(titleBounds.height)))

        let detailHeight: CGFloat
        if detail.isEmpty {
            detailHeight = 0
        } else {
            let detailBounds = NSString(string: detail).boundingRect(
                with: NSSize(width: textWidth, height: 30),
                options: [.usesLineFragmentOrigin],
                attributes: detailAttributes
            )
            detailHeight = min(24, max(11, ceil(detailBounds.height)))
        }

        let dotsHeight: CGFloat = showsProgressDots ? 7 : 0
        let height = min(94, max(42, 29 + titleHeight + detailHeight + dotsHeight))
        return NSSize(width: ceil(targetWidth), height: ceil(height))
    }

    override func draw(_ dirtyRect: NSRect) {
        super.draw(dirtyRect)

        let bubbleRect = NSRect(
            x: 6,
            y: 10,
            width: max(1, bounds.width - 12),
            height: max(1, bounds.height - 18)
        )
        let shadow = NSShadow()
        shadow.shadowColor = NSColor.black.withAlphaComponent(0.16)
        shadow.shadowBlurRadius = 8
        shadow.shadowOffset = NSSize(width: 0, height: -2)
        shadow.set()

        let fill = NSColor.windowBackgroundColor
            .blended(withFraction: 0.12, of: TaterMascotMood.themeOrange)?
            .withAlphaComponent(0.70)
            ?? NSColor.windowBackgroundColor.withAlphaComponent(0.70)
        let bubble = NSBezierPath(roundedRect: bubbleRect, xRadius: 12, yRadius: 12)
        fill.setFill()
        bubble.fill()
        NSShadow().set()

        display.mood.accentColor.withAlphaComponent(0.28).setStroke()
        bubble.lineWidth = 1.0
        bubble.stroke()

        let tail = NSBezierPath()
        let centerX = bubbleRect.midX + 14
        tail.move(to: NSPoint(x: centerX - 7, y: bubbleRect.minY + 1))
        tail.line(to: NSPoint(x: centerX + 3, y: bubbleRect.minY - 7))
        tail.line(to: NSPoint(x: centerX + 10, y: bubbleRect.minY + 1))
        tail.close()
        fill.setFill()
        tail.fill()
        display.mood.accentColor.withAlphaComponent(0.22).setStroke()
        tail.lineWidth = 0.8
        tail.stroke()

        let bottomInset: CGFloat = showsProgressDots ? 14 : 8
        let contentRect = NSRect(
            x: bubbleRect.minX + 11,
            y: bubbleRect.minY + bottomInset,
            width: max(1, bubbleRect.width - 22),
            height: max(1, bubbleRect.height - bottomInset - 7)
        )
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .center
        paragraph.lineBreakMode = .byWordWrapping

        let message = display.message.trimmingCharacters(in: .whitespacesAndNewlines)
        let detail = display.detail.trimmingCharacters(in: .whitespacesAndNewlines)
        let titleAttributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: 11, weight: .semibold),
            .foregroundColor: NSColor.labelColor.withAlphaComponent(0.92),
            .paragraphStyle: paragraph
        ]
        let detailAttributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: 9, weight: .regular),
            .foregroundColor: NSColor.secondaryLabelColor.withAlphaComponent(0.82),
            .paragraphStyle: paragraph
        ]

        let detailHeight: CGFloat = detail.isEmpty ? 0 : min(24, max(11, contentRect.height * 0.34))
        let titleHeight = max(14, contentRect.height - detailHeight - (detail.isEmpty ? 0 : 3))
        let titleRect = NSRect(
            x: contentRect.minX,
            y: contentRect.minY + detailHeight + (detail.isEmpty ? 0 : 3),
            width: contentRect.width,
            height: titleHeight
        )
        NSString(string: message.isEmpty ? "Tater is thinking" : message).draw(
            with: titleRect,
            options: [.usesLineFragmentOrigin, .truncatesLastVisibleLine],
            attributes: titleAttributes
        )

        if !detail.isEmpty {
            let detailRect = NSRect(x: contentRect.minX, y: contentRect.minY, width: contentRect.width, height: detailHeight)
            NSString(string: detail).draw(
                with: detailRect,
                options: [.usesLineFragmentOrigin, .truncatesLastVisibleLine],
                attributes: detailAttributes
            )
        }

        guard showsProgressDots else {
            return
        }
        let dotY = bubbleRect.minY + 5
        let dotStart = bubbleRect.midX - 10
        for index in 0..<3 {
            let offset = CGFloat(index) * 10
            let pulse = 0.45 + 0.45 * max(0, sin(phase * 2.2 + CGFloat(index) * 0.9))
            display.mood.accentColor.withAlphaComponent(pulse).setFill()
            NSBezierPath(ovalIn: NSRect(x: dotStart + offset, y: dotY, width: 4, height: 4)).fill()
        }
    }

    private var showsProgressDots: Bool {
        display.mood == .running || display.mood == .tool || display.mood == .starting
    }
}

private final class TaterMascotContentView: NSView {
    var onMoved: ((NSPoint) -> Void)?
    var onDoubleClick: (() -> Void)?

    private let imageView = NSImageView(frame: .zero)
    private let bubbleView = TaterMascotBubbleView(frame: .zero)
    private let idleShuffleInterval: ClosedRange<CGFloat> = 60.0...150.0
    private var mascotFrames: [NSImage] = []
    private var idleFrames: [NSImage] = []
    private var currentIdleFrameIndex = 0
    private var nextIdleShuffleAt: CGFloat = 0
    private var animationTimer: Timer?
    private var phase: CGFloat = 0
    private var lastAnimationUptime = ProcessInfo.processInfo.systemUptime
    private var dragStartPoint = NSPoint.zero
    private var dragStartFrame = NSRect.zero
    private var display = TaterMascotDisplay(mood: .idle, message: "Tater is idle", detail: "Ready when you are.")
    private let activeAnimationInterval: TimeInterval = 1.0 / 12.0
    private let idleAnimationInterval: TimeInterval = 1.0

    init(frames: [NSImage], idleFrames: [NSImage], fallbackImage: NSImage?) {
        mascotFrames = frames
        self.idleFrames = idleFrames
        if !idleFrames.isEmpty {
            currentIdleFrameIndex = Int.random(in: 0..<idleFrames.count)
            nextIdleShuffleAt = CGFloat.random(in: idleShuffleInterval)
        }
        super.init(frame: .zero)
        let initialImage = idleFrames.indices.contains(currentIdleFrameIndex) ? idleFrames[currentIdleFrameIndex] : (frames.first ?? fallbackImage)
        setup(image: initialImage)
    }

    required init?(coder: NSCoder) {
        super.init(coder: coder)
        setup(image: nil)
    }

    deinit {
        animationTimer?.invalidate()
    }

    override var isOpaque: Bool {
        false
    }

    func setDisplay(_ next: TaterMascotDisplay) {
        guard display != next else { return }
        let wasIdle = display.mood == .idle
        let wasAnimated = shouldAnimate(display.mood)
        display = next
        bubbleView.display = next
        if next.mood == .idle && !wasIdle {
            chooseNextIdleFrame()
        }
        updateMascotFrame()
        applyMascotTransform()
        if wasIdle != (next.mood == .idle) || wasAnimated != shouldAnimate(next.mood) {
            configureAnimationTimer()
        }
        needsLayout = true
    }

    private func setup(image: NSImage?) {
        wantsLayer = true
        layer?.backgroundColor = NSColor.clear.cgColor

        bubbleView.translatesAutoresizingMaskIntoConstraints = false
        addSubview(bubbleView)

        imageView.image = image
        imageView.imageScaling = .scaleProportionallyUpOrDown
        imageView.animates = false
        imageView.wantsLayer = true
        imageView.layer?.masksToBounds = false
        addSubview(imageView)

        configureAnimationTimer()
    }

    private func shouldAnimate(_ mood: TaterMascotMood) -> Bool {
        mood == .starting || mood == .running || mood == .tool
    }

    private func configureAnimationTimer() {
        animationTimer?.invalidate()
        animationTimer = nil

        let interval: TimeInterval
        if shouldAnimate(display.mood) {
            interval = activeAnimationInterval
        } else if display.mood == .idle {
            interval = idleAnimationInterval
        } else {
            return
        }

        lastAnimationUptime = ProcessInfo.processInfo.systemUptime
        let timer = Timer(timeInterval: interval, repeats: true) { [weak self] _ in
            self?.advanceAnimation()
        }
        animationTimer = timer
        RunLoop.main.add(timer, forMode: .common)
    }

    override func layout() {
        super.layout()
        let baseSide = min(bounds.width - 76, bounds.height - 104)
        imageView.frame = NSRect(
            x: (bounds.width - baseSide) / 2,
            y: 8,
            width: baseSide,
            height: baseSide
        )

        let bubbleMaxWidth = min(bounds.width - 22, 232)
        let bubbleSize = bubbleView.preferredSize(maxWidth: bubbleMaxWidth)
        let bubbleX = min(
            bounds.width - bubbleSize.width - 8,
            max(8, imageView.frame.midX - bubbleSize.width * 0.42)
        )
        let bubbleY = min(
            bounds.height - bubbleSize.height - 8,
            max(imageView.frame.maxY - 12, imageView.frame.midY + 20)
        )
        bubbleView.frame = NSRect(
            x: bubbleX,
            y: bubbleY,
            width: bubbleSize.width,
            height: bubbleSize.height
        )
        applyMascotTransform()
    }

    override func mouseDown(with event: NSEvent) {
        if event.clickCount >= 2 {
            onDoubleClick?()
            return
        }
        dragStartPoint = window?.convertPoint(toScreen: event.locationInWindow) ?? .zero
        dragStartFrame = window?.frame ?? .zero
    }

    override func mouseDragged(with event: NSEvent) {
        guard let window else { return }
        let current = window.convertPoint(toScreen: event.locationInWindow)
        let deltaX = current.x - dragStartPoint.x
        let deltaY = current.y - dragStartPoint.y
        let nextOrigin = NSPoint(x: dragStartFrame.origin.x + deltaX, y: dragStartFrame.origin.y + deltaY)
        window.setFrameOrigin(nextOrigin)
        onMoved?(nextOrigin)
    }

    private func advanceAnimation() {
        let now = ProcessInfo.processInfo.systemUptime
        phase += CGFloat(max(0, now - lastAnimationUptime))
        lastAnimationUptime = now

        if display.mood == .idle {
            if phase >= nextIdleShuffleAt {
                chooseNextIdleFrame()
                updateMascotFrame()
            }
            return
        }

        bubbleView.phase = phase
        updateMascotFrame()
    }

    private func updateMascotFrame() {
        if display.mood == .idle && !idleFrames.isEmpty {
            let safeIndex = idleFrames.indices.contains(currentIdleFrameIndex) ? currentIdleFrameIndex : 0
            let nextImage = idleFrames[safeIndex]
            if imageView.image !== nextImage {
                imageView.image = nextImage
            }
            return
        }
        guard !mascotFrames.isEmpty else { return }
        let indices = frameIndicesForCurrentMood()
        guard !indices.isEmpty else { return }
        let frameRate: CGFloat
        switch display.mood {
        case .idle:
            frameRate = 1.0
        case .starting:
            frameRate = 1.0
        case .running:
            frameRate = 1.0
        case .tool:
            frameRate = 1.0
        case .attention:
            frameRate = 1.0
        case .stopped:
            frameRate = 1.0
        }
        let offset = Int(floor(phase * frameRate)) % indices.count
        let frameIndex = indices[offset]
        if frameIndex < mascotFrames.count {
            let nextImage = mascotFrames[frameIndex]
            if imageView.image !== nextImage {
                imageView.image = nextImage
            }
        }
    }

    private func frameIndicesForCurrentMood() -> [Int] {
        let desired: [Int]
        switch display.mood {
        case .idle:
            desired = [0]
        case .starting:
            desired = [1]
        case .running:
            desired = [2]
        case .tool:
            desired = [3]
        case .attention:
            desired = [4]
        case .stopped:
            desired = [5]
        }
        let available = desired.filter { $0 >= 0 && $0 < mascotFrames.count }
        if !available.isEmpty {
            return available
        }
        return mascotFrames.indices.map { Int($0) }
    }

    private func chooseNextIdleFrame() {
        guard !idleFrames.isEmpty else { return }
        if idleFrames.count == 1 {
            currentIdleFrameIndex = 0
            scheduleNextIdleShuffle()
            return
        }

        var nextIndex = currentIdleFrameIndex
        while nextIndex == currentIdleFrameIndex {
            nextIndex = Int.random(in: 0..<idleFrames.count)
        }
        currentIdleFrameIndex = nextIndex
        scheduleNextIdleShuffle()
    }

    private func scheduleNextIdleShuffle() {
        nextIdleShuffleAt = phase + CGFloat.random(in: idleShuffleInterval)
    }

    private func applyMascotTransform() {
        imageView.layer?.setAffineTransform(.identity)
        imageView.alphaValue = display.mood == .stopped ? 0.82 : 1.0
    }
}

private final class TaterMascotController {
    private let backend: BackendManager
    private let defaults = UserDefaults.standard
    private var panel: NSPanel?
    private var mascotView: TaterMascotContentView?
    private var pollTimer: Timer?
    private var backendState: BackendState = .stopped
    private var enabled = false

    private let originXKey = "TaterMascotOriginX"
    private let originYKey = "TaterMascotOriginY"
    private let panelSize = NSSize(width: 260, height: 260)

    init(backend: BackendManager) {
        self.backend = backend
    }

    func setEnabled(_ next: Bool) {
        guard enabled != next else {
            if next {
                refreshForBackendState()
            }
            return
        }
        enabled = next
        if next {
            show()
        } else {
            pollTimer?.invalidate()
            pollTimer = nil
            panel?.orderOut(nil)
        }
    }

    func handleBackendState(_ state: BackendState) {
        backendState = state
        guard enabled else { return }
        refreshForBackendState()
    }

    private func show() {
        if panel == nil {
            makePanel()
        }
        panel?.orderFrontRegardless()
        refreshForBackendState()
    }

    private func makePanel() {
        let frame = NSRect(origin: initialOrigin(), size: panelSize)
        let panel = NSPanel(
            contentRect: frame,
            styleMask: [.borderless, .nonactivatingPanel],
            backing: .buffered,
            defer: false
        )
        panel.isOpaque = false
        panel.backgroundColor = .clear
        panel.hasShadow = false
        panel.hidesOnDeactivate = false
        panel.isMovableByWindowBackground = false
        panel.isReleasedWhenClosed = false
        panel.level = .floating
        panel.collectionBehavior = [.canJoinAllSpaces, .fullScreenAuxiliary, .stationary]

        let contentView = TaterMascotContentView(
            frames: mascotFrames(),
            idleFrames: mascotIdleFrames(),
            fallbackImage: fallbackMascotImage()
        )
        contentView.frame = NSRect(origin: .zero, size: panelSize)
        contentView.autoresizingMask = [.width, .height]
        contentView.onMoved = { [weak self] origin in
            self?.save(origin: origin)
        }
        contentView.onDoubleClick = { [weak self] in
            NSApp.activate(ignoringOtherApps: true)
            self?.panel?.orderFrontRegardless()
        }
        panel.contentView = contentView

        self.panel = panel
        self.mascotView = contentView
    }

    private func mascotFrames() -> [NSImage] {
        var frames: [NSImage] = []
        for index in 0..<12 {
            let name = String(format: "frame-%02d", index)
            guard
                let url = Bundle.main.url(forResource: name, withExtension: "png", subdirectory: "TaterMascotFrames"),
                let image = NSImage(contentsOf: url)
            else {
                continue
            }
            frames.append(image)
        }
        return frames
    }

    private func mascotIdleFrames() -> [NSImage] {
        var frames: [NSImage] = []
        for index in 0..<8 {
            let name = String(format: "idle-%02d", index)
            guard
                let url = Bundle.main.url(forResource: name, withExtension: "png", subdirectory: "TaterMascotIdleFrames"),
                let image = NSImage(contentsOf: url)
            else {
                continue
            }
            frames.append(image)
        }
        return frames
    }

    private func fallbackMascotImage() -> NSImage? {
        if let url = Bundle.main.url(forResource: "TaterAvatar", withExtension: "png"),
           let image = NSImage(contentsOf: url) {
            return image
        }
        return nil
    }

    private func initialOrigin() -> NSPoint {
        if
            defaults.object(forKey: originXKey) != nil,
            defaults.object(forKey: originYKey) != nil
        {
            let saved = NSPoint(x: defaults.double(forKey: originXKey), y: defaults.double(forKey: originYKey))
            let savedFrame = NSRect(origin: saved, size: panelSize)
            if NSScreen.screens.contains(where: { $0.visibleFrame.intersects(savedFrame) }) {
                return saved
            }
        }

        let screenFrame = NSScreen.main?.visibleFrame ?? NSRect(x: 80, y: 80, width: 1200, height: 800)
        return NSPoint(
            x: screenFrame.maxX - panelSize.width - 28,
            y: screenFrame.minY + 34
        )
    }

    private func save(origin: NSPoint) {
        defaults.set(Double(origin.x), forKey: originXKey)
        defaults.set(Double(origin.y), forKey: originYKey)
    }

    private func refreshForBackendState() {
        switch backendState {
        case .running:
            schedulePolling()
            pollStatus()
        case .bootstrapping:
            stopPolling()
            mascotView?.setDisplay(TaterMascotDisplay(
                mood: .starting,
                message: "Setting up Tater",
                detail: "Preparing the runtime"
            ))
        case .starting:
            stopPolling()
            mascotView?.setDisplay(TaterMascotDisplay(
                mood: .starting,
                message: "Starting Tater",
                detail: "Opening the local service"
            ))
        case .stopped:
            stopPolling()
            mascotView?.setDisplay(TaterMascotDisplay(
                mood: .stopped,
                message: "Tater is stopped",
                detail: "Use the menu bar to start"
            ))
        case .failed(let message):
            stopPolling()
            mascotView?.setDisplay(TaterMascotDisplay(
                mood: .attention,
                message: "Tater needs attention",
                detail: shortText(message, limit: 44)
            ))
        }
    }

    private func schedulePolling() {
        guard pollTimer == nil else { return }
        let timer = Timer(timeInterval: 1.4, repeats: true) { [weak self] _ in
            self?.pollStatus()
        }
        pollTimer = timer
        RunLoop.main.add(timer, forMode: .common)
    }

    private func stopPolling() {
        pollTimer?.invalidate()
        pollTimer = nil
    }

    private func pollStatus() {
        guard enabled else { return }
        guard case .running = backendState else { return }
        guard let url = URL(string: "/api/native/mascot/status", relativeTo: backend.webURL)?.absoluteURL else {
            return
        }

        var request = URLRequest(url: url)
        request.timeoutInterval = 2.0
        request.setValue(backend.nativeStatusToken, forHTTPHeaderField: "X-Tater-Native-Token")
        request.setValue("application/json", forHTTPHeaderField: "Accept")

        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            guard let self else { return }
            let display: TaterMascotDisplay
            if error != nil {
                display = TaterMascotDisplay(mood: .running, message: "Watching Tater", detail: "Status feed is warming up")
            } else if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
                display = TaterMascotDisplay(mood: .running, message: "Watching Tater", detail: "Waiting for native status")
            } else if let data, let decoded = try? JSONDecoder().decode(NativeMascotStatus.self, from: data) {
                display = self.display(from: decoded)
            } else {
                display = TaterMascotDisplay(mood: .idle, message: "Tater is idle", detail: "Ready when you are.")
            }

            DispatchQueue.main.async {
                guard self.enabled else { return }
                self.mascotView?.setDisplay(display)
            }
        }.resume()
    }

    private func display(from status: NativeMascotStatus) -> TaterMascotDisplay {
        let state = (status.state ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let mood: TaterMascotMood
        switch state {
        case "tool":
            mood = .tool
        case "running":
            mood = .running
        case "idle":
            mood = .idle
        case "attention", "error", "failed":
            mood = .attention
        case "stopped":
            mood = .stopped
        default:
            mood = .running
        }

        let activeCount = max(0, status.activeCount ?? 0)
        let baseDetail = shortText(status.detail ?? "", limit: 72)
        let detail: String
        if activeCount > 1 {
            detail = baseDetail.isEmpty ? "\(activeCount) active tasks" : "\(baseDetail) - \(activeCount) active"
        } else {
            detail = baseDetail
        }

        return TaterMascotDisplay(
            mood: mood,
            message: shortText(status.message ?? "Tater is working", limit: 96),
            detail: detail.isEmpty ? "Ready when you are." : shortText(detail, limit: 72)
        )
    }

    private func shortText(_ raw: String, limit: Int) -> String {
        let compact = raw.split(whereSeparator: { $0.isWhitespace }).joined(separator: " ")
        guard compact.count > limit else { return compact }
        let end = compact.index(compact.startIndex, offsetBy: max(0, limit - 1))
        return String(compact[..<end]) + "..."
    }
}

private final class TaterWindowController: NSWindowController, WKNavigationDelegate, WKUIDelegate {
    private let webView: WKWebView
    private let setupView: NSVisualEffectView
    private let setupLogoView: NSImageView
    private let progressIndicator: NSProgressIndicator
    private let statusTitleLabel: NSTextField
    private let statusDetailLabel: NSTextField
    private var outputBuffer = ""

    init() {
        webView = WKWebView(frame: .zero)
        setupView = NSVisualEffectView(frame: .zero)
        setupLogoView = NSImageView(frame: .zero)
        progressIndicator = NSProgressIndicator(frame: .zero)
        statusTitleLabel = NSTextField(labelWithString: "Starting Tater")
        statusDetailLabel = NSTextField(labelWithString: "Preparing the local runtime...")

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1500, height: 980),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.title = "Tater"
        window.minSize = NSSize(width: 1100, height: 720)
        window.center()
        window.backgroundColor = .clear

        super.init(window: window)

        webView.navigationDelegate = self
        webView.uiDelegate = self

        let contentView = NSView()
        contentView.translatesAutoresizingMaskIntoConstraints = false
        webView.translatesAutoresizingMaskIntoConstraints = false
        setupView.translatesAutoresizingMaskIntoConstraints = false
        setupLogoView.translatesAutoresizingMaskIntoConstraints = false
        progressIndicator.translatesAutoresizingMaskIntoConstraints = false
        statusTitleLabel.translatesAutoresizingMaskIntoConstraints = false
        statusDetailLabel.translatesAutoresizingMaskIntoConstraints = false

        setupView.material = .hudWindow
        setupView.blendingMode = .withinWindow
        setupView.state = .active
        setupView.wantsLayer = true

        setupLogoView.image = bundledImage(named: "TaterSetupLogo", withExtension: "png")
        setupLogoView.imageScaling = .scaleProportionallyUpOrDown
        setupLogoView.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        setupLogoView.setContentCompressionResistancePriority(.defaultLow, for: .vertical)

        progressIndicator.style = .spinning
        progressIndicator.controlSize = .regular
        progressIndicator.startAnimation(nil)

        statusTitleLabel.font = NSFont.systemFont(ofSize: 22, weight: .semibold)
        statusTitleLabel.textColor = .labelColor
        statusTitleLabel.alignment = .center
        statusDetailLabel.font = NSFont.systemFont(ofSize: 14, weight: .regular)
        statusDetailLabel.textColor = .secondaryLabelColor
        statusDetailLabel.alignment = .center
        statusDetailLabel.lineBreakMode = .byTruncatingMiddle

        setupView.addSubview(setupLogoView)
        setupView.addSubview(progressIndicator)
        setupView.addSubview(statusTitleLabel)
        setupView.addSubview(statusDetailLabel)
        contentView.addSubview(webView)
        contentView.addSubview(setupView)
        window.contentView = contentView

        NSLayoutConstraint.activate([
            webView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
            webView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
            webView.topAnchor.constraint(equalTo: contentView.topAnchor),
            webView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor),
            setupView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
            setupView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
            setupView.topAnchor.constraint(equalTo: contentView.topAnchor),
            setupView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor),
            setupLogoView.centerXAnchor.constraint(equalTo: setupView.centerXAnchor),
            setupLogoView.centerYAnchor.constraint(equalTo: setupView.centerYAnchor, constant: -58),
            setupLogoView.widthAnchor.constraint(lessThanOrEqualTo: setupView.widthAnchor, multiplier: 0.76),
            setupLogoView.heightAnchor.constraint(lessThanOrEqualTo: setupView.heightAnchor, multiplier: 0.42),
            setupLogoView.widthAnchor.constraint(lessThanOrEqualToConstant: 760),
            progressIndicator.centerXAnchor.constraint(equalTo: setupView.centerXAnchor),
            progressIndicator.topAnchor.constraint(equalTo: setupLogoView.bottomAnchor, constant: 32),
            statusTitleLabel.leadingAnchor.constraint(equalTo: setupView.leadingAnchor, constant: 56),
            statusTitleLabel.trailingAnchor.constraint(equalTo: setupView.trailingAnchor, constant: -56),
            statusTitleLabel.topAnchor.constraint(equalTo: progressIndicator.bottomAnchor, constant: 20),
            statusDetailLabel.leadingAnchor.constraint(equalTo: setupView.leadingAnchor, constant: 72),
            statusDetailLabel.trailingAnchor.constraint(equalTo: setupView.trailingAnchor, constant: -72),
            statusDetailLabel.topAnchor.constraint(equalTo: statusTitleLabel.bottomAnchor, constant: 10)
        ])

        setStatus("Starting Tater", detail: "Preparing the local runtime...", webVisible: false)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func load(url: URL) {
        var request = URLRequest(
            url: url,
            cachePolicy: .reloadIgnoringLocalAndRemoteCacheData,
            timeoutInterval: 30.0
        )
        request.setValue("no-cache", forHTTPHeaderField: "Cache-Control")
        webView.load(request)
    }

    func webView(
        _ webView: WKWebView,
        decidePolicyFor navigationAction: WKNavigationAction,
        decisionHandler: @escaping (WKNavigationActionPolicy) -> Void
    ) {
        guard let url = navigationAction.request.url else {
            decisionHandler(.allow)
            return
        }
        if shouldOpenExternally(url) {
            NSWorkspace.shared.open(url)
            decisionHandler(.cancel)
            return
        }
        decisionHandler(.allow)
    }

    func webView(
        _ webView: WKWebView,
        createWebViewWith configuration: WKWebViewConfiguration,
        for navigationAction: WKNavigationAction,
        windowFeatures: WKWindowFeatures
    ) -> WKWebView? {
        if let url = navigationAction.request.url {
            NSWorkspace.shared.open(url)
        }
        return nil
    }

    func webView(
        _ webView: WKWebView,
        runJavaScriptAlertPanelWithMessage message: String,
        initiatedByFrame frame: WKFrameInfo,
        completionHandler: @escaping () -> Void
    ) {
        let alert = NSAlert()
        alert.messageText = "Tater"
        alert.informativeText = message
        alert.alertStyle = .informational
        alert.addButton(withTitle: "OK")
        presentJavaScriptAlert(alert, webView: webView) { _ in
            completionHandler()
        }
    }

    func webView(
        _ webView: WKWebView,
        runOpenPanelWith parameters: WKOpenPanelParameters,
        initiatedByFrame frame: WKFrameInfo,
        completionHandler: @escaping ([URL]?) -> Void
    ) {
        let panel = NSOpenPanel()
        panel.canChooseFiles = true
        if #available(macOS 10.13.4, *) {
            panel.canChooseDirectories = parameters.allowsDirectories
        } else {
            panel.canChooseDirectories = false
        }
        panel.allowsMultipleSelection = parameters.allowsMultipleSelection
        panel.canCreateDirectories = false
        panel.resolvesAliases = true
        panel.message = parameters.allowsMultipleSelection
            ? "Choose files to attach to Tater."
            : "Choose a file to attach to Tater."

        if let window = webView.window ?? self.window {
            panel.beginSheetModal(for: window) { response in
                completionHandler(response == .OK ? panel.urls : nil)
            }
        } else {
            let response = panel.runModal()
            completionHandler(response == .OK ? panel.urls : nil)
        }
    }

    func webView(
        _ webView: WKWebView,
        runJavaScriptConfirmPanelWithMessage message: String,
        initiatedByFrame frame: WKFrameInfo,
        completionHandler: @escaping (Bool) -> Void
    ) {
        let alert = NSAlert()
        alert.messageText = "Tater"
        alert.informativeText = message
        alert.alertStyle = .warning
        alert.addButton(withTitle: "OK")
        alert.addButton(withTitle: "Cancel")
        presentJavaScriptAlert(alert, webView: webView) { response in
            completionHandler(response == .alertFirstButtonReturn)
        }
    }

    func webView(
        _ webView: WKWebView,
        runJavaScriptTextInputPanelWithPrompt prompt: String,
        defaultText: String?,
        initiatedByFrame frame: WKFrameInfo,
        completionHandler: @escaping (String?) -> Void
    ) {
        let input = NSTextField(frame: NSRect(x: 0, y: 0, width: 320, height: 24))
        input.stringValue = defaultText ?? ""

        let alert = NSAlert()
        alert.messageText = "Tater"
        alert.informativeText = prompt
        alert.alertStyle = .informational
        alert.accessoryView = input
        alert.addButton(withTitle: "OK")
        alert.addButton(withTitle: "Cancel")
        presentJavaScriptAlert(alert, webView: webView) { response in
            completionHandler(response == .alertFirstButtonReturn ? input.stringValue : nil)
        }
    }

    private func presentJavaScriptAlert(
        _ alert: NSAlert,
        webView: WKWebView,
        completion: @escaping (NSApplication.ModalResponse) -> Void
    ) {
        if let window = webView.window ?? self.window {
            alert.beginSheetModal(for: window) { response in
                completion(response)
            }
        } else {
            completion(alert.runModal())
        }
    }

    private func shouldOpenExternally(_ url: URL) -> Bool {
        guard let scheme = url.scheme?.lowercased() else {
            return false
        }
        if scheme != "http" && scheme != "https" {
            return !["about", "data", "blob"].contains(scheme)
        }
        let host = url.host?.lowercased() ?? ""
        let localHosts: Set<String> = ["127.0.0.1", "localhost", "::1"]
        if localHosts.contains(host), url.port == taterPort || url.port == nil {
            return false
        }
        return true
    }

    func setStatus(_ title: String, detail: String = "", webVisible: Bool) {
        statusTitleLabel.stringValue = title
        if !detail.isEmpty {
            statusDetailLabel.stringValue = detail
        }
        setupView.isHidden = webVisible
        webView.isHidden = !webVisible
        if webVisible {
            progressIndicator.stopAnimation(nil)
        } else {
            progressIndicator.startAnimation(nil)
        }
    }

    func updateSetupProgress(from text: String) {
        guard !text.isEmpty else { return }
        outputBuffer += text
        let lines = outputBuffer.components(separatedBy: .newlines)
        outputBuffer = lines.last ?? ""
        for line in lines.dropLast() {
            if let status = setupStatus(from: line) {
                statusDetailLabel.stringValue = status
            }
        }
    }

    private func setupStatus(from rawLine: String) -> String? {
        let line = rawLine.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !line.isEmpty else { return nil }
        if line.hasPrefix("==> ") {
            return String(line.dropFirst(4))
        }
        if line.hasPrefix("OK  ") {
            return String(line.dropFirst(4))
        }
        if line.hasPrefix("WARN ") {
            return String(line.dropFirst(5))
        }
        if line.hasPrefix("Collecting ") {
            return "Installing \(packageName(from: line.dropFirst(11)))"
        }
        if line.hasPrefix("Using cached ") {
            return "Preparing cached package \(packageName(from: line.dropFirst(13)))"
        }
        if line.hasPrefix("Downloading ") {
            return "Downloading \(packageName(from: line.dropFirst(12)))"
        }
        if line.hasPrefix("Installing collected packages:") {
            return "Installing Python packages..."
        }
        if line.hasPrefix("Successfully installed") {
            return "Python packages installed"
        }
        if line.hasPrefix("Cloning into ") {
            return "Downloading native runtime..."
        }
        if line.contains("core imports ok") {
            return "Checking installed runtime..."
        }
        if line.contains("TaterOS backend started") {
            return "Tater is ready"
        }
        if line.contains("Uvicorn running on") {
            return "Opening Tater..."
        }
        return nil
    }

    private func packageName<S: StringProtocol>(from value: S) -> String {
        let token = value
            .split(separator: " ", maxSplits: 1)
            .first?
            .split(separator: "=", maxSplits: 1)
            .first?
            .trimmingCharacters(in: CharacterSet(charactersIn: ",;:()"))
        return token.flatMap { $0.isEmpty ? nil : String($0) } ?? "dependency"
    }

    private func bundledImage(named name: String, withExtension ext: String) -> NSImage? {
        guard let url = Bundle.main.url(forResource: name, withExtension: ext) else {
            return nil
        }
        return NSImage(contentsOf: url)
    }
}

private final class DataMigrationCancellation {
    private let lock = NSLock()
    private var cancelled = false

    func cancel() {
        lock.lock()
        cancelled = true
        lock.unlock()
    }

    var isCancelled: Bool {
        lock.lock()
        defer { lock.unlock() }
        return cancelled
    }
}

private struct DataMigrationStats {
    let entryCount: Int
    let fileCount: Int
    let byteCount: Int64
}

private struct DataMigrationCancelled: LocalizedError {
    var errorDescription: String? { "The data move was cancelled. Your original data was not changed." }
}

private final class DataMigrationWindowController: NSWindowController {
    private let titleLabel = NSTextField(labelWithString: "Moving Tater Data")
    private let statusLabel = NSTextField(labelWithString: "Preparing…")
    private let detailLabel = NSTextField(labelWithString: "")
    private let pathLabel = NSTextField(wrappingLabelWithString: "")
    private let progress = NSProgressIndicator()
    private let cancelButton = NSButton(title: "Cancel", target: nil, action: nil)
    var onCancel: (() -> Void)?

    init(source: URL, destination: URL) {
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 560, height: 330),
            styleMask: [.titled, .closable, .fullSizeContentView],
            backing: .buffered,
            defer: false
        )
        window.title = "Move Tater Data"
        window.titlebarAppearsTransparent = true
        window.isMovableByWindowBackground = true
        window.standardWindowButton(.closeButton)?.isEnabled = false
        window.center()
        super.init(window: window)

        let icon = NSImageView()
        icon.image = NSImage(systemSymbolName: "externaldrive.badge.plus", accessibilityDescription: "Move data")
        icon.symbolConfiguration = NSImage.SymbolConfiguration(pointSize: 34, weight: .medium)
        icon.contentTintColor = .systemPurple
        icon.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            icon.widthAnchor.constraint(equalToConstant: 44),
            icon.heightAnchor.constraint(equalToConstant: 44),
        ])

        titleLabel.font = .systemFont(ofSize: 24, weight: .bold)
        statusLabel.font = .systemFont(ofSize: 15, weight: .semibold)
        detailLabel.textColor = .secondaryLabelColor
        pathLabel.textColor = .secondaryLabelColor
        pathLabel.font = .monospacedSystemFont(ofSize: 11, weight: .regular)
        pathLabel.stringValue = "\(source.path)\n→ \(destination.path)"
        pathLabel.maximumNumberOfLines = 3

        progress.isIndeterminate = true
        progress.style = .bar
        progress.controlSize = .large
        progress.startAnimation(nil)

        cancelButton.target = self
        cancelButton.action = #selector(cancelPressed)
        cancelButton.bezelStyle = .rounded

        let headerText = NSStackView(views: [titleLabel, statusLabel])
        headerText.orientation = .vertical
        headerText.alignment = .leading
        headerText.spacing = 5
        let header = NSStackView(views: [icon, headerText])
        header.orientation = .horizontal
        header.alignment = .centerY
        header.spacing = 14

        let buttonRow = NSStackView(views: [NSView(), cancelButton])
        buttonRow.orientation = .horizontal
        buttonRow.alignment = .centerY

        let content = NSStackView(views: [header, pathLabel, progress, detailLabel, buttonRow])
        content.orientation = .vertical
        content.alignment = .leading
        content.spacing = 18
        content.edgeInsets = NSEdgeInsets(top: 34, left: 36, bottom: 28, right: 36)
        content.translatesAutoresizingMaskIntoConstraints = false
        window.contentView = NSView()
        window.contentView?.addSubview(content)
        NSLayoutConstraint.activate([
            content.leadingAnchor.constraint(equalTo: window.contentView!.leadingAnchor),
            content.trailingAnchor.constraint(equalTo: window.contentView!.trailingAnchor),
            content.topAnchor.constraint(equalTo: window.contentView!.topAnchor),
            content.bottomAnchor.constraint(equalTo: window.contentView!.bottomAnchor),
            pathLabel.widthAnchor.constraint(equalTo: content.widthAnchor, constant: -72),
            progress.widthAnchor.constraint(equalTo: content.widthAnchor, constant: -72),
            detailLabel.widthAnchor.constraint(equalTo: content.widthAnchor, constant: -72),
            buttonRow.widthAnchor.constraint(equalTo: content.widthAnchor, constant: -72),
        ])
    }

    required init?(coder: NSCoder) { nil }

    @objc private func cancelPressed() {
        cancelButton.isEnabled = false
        statusLabel.stringValue = "Cancelling safely…"
        detailLabel.stringValue = "Finishing the current file and removing the incomplete copy."
        onCancel?()
    }

    func setScanning() {
        statusLabel.stringValue = "Measuring your Tater data"
        detailLabel.stringValue = "Checking files and available space before copying."
        progress.isIndeterminate = true
        progress.startAnimation(nil)
    }

    func setCopying(copied: Int64, total: Int64, files: Int, totalFiles: Int) {
        progress.stopAnimation(nil)
        progress.isIndeterminate = false
        progress.minValue = 0
        progress.maxValue = 1
        progress.doubleValue = total > 0 ? min(1, Double(copied) / Double(total)) : 1
        statusLabel.stringValue = "Copying Tater data"
        let formatter = ByteCountFormatter()
        formatter.countStyle = .file
        let copiedText = formatter.string(fromByteCount: copied)
        let totalText = formatter.string(fromByteCount: total)
        detailLabel.stringValue = "\(copiedText) of \(totalText)  •  \(files) of \(totalFiles) files"
    }

    func setSwitching(_ text: String) {
        cancelButton.isEnabled = false
        progress.isIndeterminate = true
        progress.startAnimation(nil)
        statusLabel.stringValue = text
        detailLabel.stringValue = "The original data stays available until Tater starts successfully."
    }

    func finish(success: Bool, title: String, detail: String) {
        progress.stopAnimation(nil)
        progress.isIndeterminate = false
        progress.doubleValue = success ? 1 : 0
        titleLabel.stringValue = success ? "Tater Data Moved" : "Data Move Stopped"
        statusLabel.stringValue = title
        detailLabel.stringValue = detail
        cancelButton.title = "Close"
        cancelButton.isEnabled = true
        window?.standardWindowButton(.closeButton)?.isEnabled = true
        cancelButton.target = window
        cancelButton.action = #selector(NSWindow.performClose(_:))
    }
}

private final class AppDelegate: NSObject, NSApplicationDelegate {
    private let backend = BackendManager()
    private let updater = UpdateManager()
    private var statusItem: NSStatusItem?
    private var statusMenuItem: NSMenuItem?
    private var startMenuItem: NSMenuItem?
    private var restartMenuItem: NSMenuItem?
    private var stopMenuItem: NSMenuItem?
    private var updateMenuItem: NSMenuItem?
    private var checkUpdatesMenuItem: NSMenuItem?
    private var dataMigrationMenuItem: NSMenuItem?
    private var mascotMenuItem: NSMenuItem?
    private var windowController: TaterWindowController?
    private var mascotController: TaterMascotController?
    private var usesMenuBarImage = false
    private var recoveryTimer: Timer?
    private var updateMenuResetTimer: Timer?
    private var updateCheckTimer: Timer?
    private let automaticUpdateInterval: TimeInterval = 12 * 60 * 60
    private let lastAutomaticUpdateCheckKey = "LastAutomaticUpdateCheck"
    private let mascotEnabledKey = "TaterMascotEnabled"
    private var mascotEnabled = false
    private var terminationInProgress = false
    private var migrationInProgress = false
    private var migrationWindowController: DataMigrationWindowController?
    private var migrationCancellation: DataMigrationCancellation?

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
        mascotController = TaterMascotController(backend: backend)
        configureStatusItem()

        backend.onStateChange = { [weak self] state in
            self?.refreshMenu(for: state)
            self?.refreshWindow(for: state)
            self?.mascotController?.handleBackendState(state)
        }
        backend.onLogAppend = { [weak self] text in
            self?.windowController?.updateSetupProgress(from: text)
        }
        updater.onStateChange = { [weak self] state in
            self?.refreshUpdateMenu(for: state)
        }

        showWindow()
        mascotEnabled = UserDefaults.standard.object(forKey: mascotEnabledKey) as? Bool ?? false
        mascotController?.setEnabled(mascotEnabled)
        refreshMascotMenu()
        backend.start()
        startRecoveryWatchdog()
        startAutomaticUpdateChecks()
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        false
    }

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        if migrationInProgress {
            let alert = NSAlert()
            alert.messageText = "Tater is still moving data"
            alert.informativeText = "Wait for the move to finish, or cancel it in the migration window before quitting. This keeps both the original and new copies safe."
            alert.alertStyle = .warning
            alert.addButton(withTitle: "Keep Tater Open")
            alert.runModal()
            return .terminateCancel
        }
        if terminationInProgress {
            return .terminateLater
        }
        terminationInProgress = true
        recoveryTimer?.invalidate()
        updateCheckTimer?.invalidate()
        updateMenuResetTimer?.invalidate()
        mascotController?.setEnabled(false)
        DispatchQueue.global(qos: .userInitiated).async { [weak self, weak sender] in
            self?.backend.stop(waitForExit: true)
            guard let sender else { return }
            let mainRunLoop = CFRunLoopGetMain()
            CFRunLoopPerformBlock(
                mainRunLoop,
                RunLoop.Mode.modalPanel.rawValue as CFString
            ) {
                sender.reply(toApplicationShouldTerminate: true)
            }
            CFRunLoopWakeUp(mainRunLoop)
        }
        return .terminateLater
    }

    private func startRecoveryWatchdog() {
        recoveryTimer?.invalidate()
        recoveryTimer = Timer.scheduledTimer(withTimeInterval: 2.0, repeats: true) { [weak self] _ in
            self?.backend.recoverIfBackendMissing()
        }
    }

    private func startAutomaticUpdateChecks() {
        updateCheckTimer?.invalidate()
        scheduleAutomaticUpdateCheck(after: automaticUpdateCheckDelay())
    }

    private func automaticUpdateCheckDelay() -> TimeInterval {
        let lastCheck = UserDefaults.standard.double(forKey: lastAutomaticUpdateCheckKey)
        guard lastCheck > 0 else {
            return 6
        }

        let elapsed = Date().timeIntervalSince1970 - lastCheck
        guard elapsed >= 0 else {
            return automaticUpdateInterval
        }

        if elapsed >= automaticUpdateInterval {
            return 6
        }
        return max(6, automaticUpdateInterval - elapsed)
    }

    private func scheduleAutomaticUpdateCheck(after delay: TimeInterval) {
        updateCheckTimer?.invalidate()
        updateCheckTimer = Timer.scheduledTimer(withTimeInterval: delay, repeats: false) { [weak self] _ in
            self?.runAutomaticUpdateCheck()
        }
    }

    private func runAutomaticUpdateCheck() {
        if updater.state.isBusy {
            scheduleAutomaticUpdateCheck(after: 60)
            return
        }

        UserDefaults.standard.set(Date().timeIntervalSince1970, forKey: lastAutomaticUpdateCheckKey)
        updater.checkForUpdates(manual: false)
        scheduleAutomaticUpdateCheck(after: automaticUpdateInterval)
    }

    private func configureStatusItem() {
        let item = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        if let image = resourceImage(named: "TaterMenuBarTemplate", withExtension: "png") {
            image.isTemplate = true
            image.size = NSSize(width: 18, height: 18)
            item.button?.image = image
            item.button?.imagePosition = .imageOnly
            item.button?.title = ""
            usesMenuBarImage = true
        } else {
            item.button?.title = "T"
            usesMenuBarImage = false
        }
        item.button?.toolTip = "Tater"

        let menu = NSMenu()
        let status = NSMenuItem(title: "Status: \(backend.state.label)", action: nil, keyEquivalent: "")
        status.isEnabled = false
        menu.addItem(status)
        let update = NSMenuItem(title: "Update Available", action: #selector(installUpdate), keyEquivalent: "")
        update.target = self
        update.isHidden = true
        menu.addItem(update)
        menu.addItem(NSMenuItem.separator())
        menu.addItem(NSMenuItem(title: "Open Tater", action: #selector(openTater), keyEquivalent: "o"))
        menu.addItem(NSMenuItem(title: "Open in Browser", action: #selector(openBrowser), keyEquivalent: "b"))
        let mascot = NSMenuItem(title: "Enable Tater Mascot", action: #selector(toggleMascot), keyEquivalent: "m")
        mascot.target = self
        menu.addItem(mascot)
        menu.addItem(NSMenuItem.separator())
        let start = NSMenuItem(title: "Start", action: #selector(startTater), keyEquivalent: "s")
        let restart = NSMenuItem(title: "Restart", action: #selector(restartTater), keyEquivalent: "r")
        let stop = NSMenuItem(title: "Stop", action: #selector(stopTater), keyEquivalent: "")
        menu.addItem(start)
        menu.addItem(restart)
        menu.addItem(stop)
        menu.addItem(NSMenuItem.separator())
        menu.addItem(NSMenuItem(title: "Show Logs", action: #selector(showLogs), keyEquivalent: "l"))
        let moveData = NSMenuItem(title: "Move Tater Data…", action: #selector(moveTaterData), keyEquivalent: "")
        moveData.target = self
        menu.addItem(moveData)
        let checkUpdates = NSMenuItem(title: "Check for Updates...", action: #selector(checkForUpdates), keyEquivalent: "")
        checkUpdates.target = self
        menu.addItem(checkUpdates)
        menu.addItem(NSMenuItem.separator())
        menu.addItem(NSMenuItem(title: "Quit Tater", action: #selector(quit), keyEquivalent: "q"))

        item.menu = menu
        statusItem = item
        statusMenuItem = status
        startMenuItem = start
        restartMenuItem = restart
        stopMenuItem = stop
        updateMenuItem = update
        checkUpdatesMenuItem = checkUpdates
        dataMigrationMenuItem = moveData
        mascotMenuItem = mascot
        refreshMenu(for: backend.state)
        refreshUpdateMenu(for: updater.state)
        refreshMascotMenu()
    }

    private func refreshMenu(for state: BackendState) {
        statusMenuItem?.title = "Status: \(state.label)"
        switch state {
        case .running, .starting, .bootstrapping:
            startMenuItem?.isEnabled = false
            stopMenuItem?.isEnabled = true
            if !usesMenuBarImage {
                statusItem?.button?.title = "T"
            }
        case .stopped, .failed:
            startMenuItem?.isEnabled = true
            stopMenuItem?.isEnabled = false
            if !usesMenuBarImage {
                statusItem?.button?.title = "T"
            }
        }
        dataMigrationMenuItem?.isEnabled = !migrationInProgress
        if migrationInProgress {
            startMenuItem?.isEnabled = false
            restartMenuItem?.isEnabled = false
            stopMenuItem?.isEnabled = false
        } else {
            restartMenuItem?.isEnabled = true
        }
    }

    private func refreshUpdateMenu(for state: UpdateState) {
        updateMenuResetTimer?.invalidate()
        checkUpdatesMenuItem?.isEnabled = true
        checkUpdatesMenuItem?.title = "Check for Updates..."

        switch state {
        case .idle:
            hideUpdateItem()
        case .checking:
            hideUpdateItem()
            checkUpdatesMenuItem?.title = "Checking for Updates..."
            checkUpdatesMenuItem?.isEnabled = false
        case .current:
            hideUpdateItem()
            checkUpdatesMenuItem?.title = "Tater is Up to Date"
            resetCheckUpdateTitleSoon()
        case .available(let manifest):
            showOrangeUpdateItem("Update Available: \(manifest.version)", enabled: true)
        case .downloading(let manifest):
            showOrangeUpdateItem("Downloading Tater \(manifest.version)...", enabled: false)
            checkUpdatesMenuItem?.isEnabled = false
        case .installing(let manifest):
            showOrangeUpdateItem("Installing Tater \(manifest.version)...", enabled: false)
            checkUpdatesMenuItem?.isEnabled = false
        case .failed:
            hideUpdateItem()
            checkUpdatesMenuItem?.title = "Update Check Failed"
            resetCheckUpdateTitleSoon()
        }
    }

    private func showOrangeUpdateItem(_ title: String, enabled: Bool) {
        guard let updateMenuItem else { return }
        updateMenuItem.isHidden = false
        updateMenuItem.isEnabled = enabled
        updateMenuItem.attributedTitle = NSAttributedString(
            string: title,
            attributes: [
                .foregroundColor: NSColor.systemOrange,
                .font: NSFont.menuFont(ofSize: NSFont.systemFontSize)
            ]
        )
    }

    private func hideUpdateItem() {
        updateMenuItem?.isHidden = true
        updateMenuItem?.isEnabled = false
        updateMenuItem?.attributedTitle = nil
        updateMenuItem?.title = "Update Available"
    }

    private func refreshMascotMenu() {
        mascotMenuItem?.title = mascotEnabled ? "Disable Tater Mascot" : "Enable Tater Mascot"
        mascotMenuItem?.state = mascotEnabled ? .on : .off
    }

    private func resetCheckUpdateTitleSoon() {
        updateMenuResetTimer = Timer.scheduledTimer(withTimeInterval: 4.0, repeats: false) { [weak self] _ in
            self?.checkUpdatesMenuItem?.title = "Check for Updates..."
        }
    }

    private func resourceImage(named name: String, withExtension ext: String) -> NSImage? {
        guard let url = Bundle.main.url(forResource: name, withExtension: ext) else {
            return nil
        }
        return NSImage(contentsOf: url)
    }

    private func refreshWindow(for state: BackendState) {
        switch state {
        case .running:
            windowController?.setStatus("", webVisible: true)
            windowController?.load(url: backend.webURL)
        case .bootstrapping:
            windowController?.setStatus(
                "Setting up Tater",
                detail: "Installing the private Python runtime in \(backend.supportRoot.path)",
                webVisible: false
            )
        case .starting:
            windowController?.setStatus(
                "Starting Tater",
                detail: "Launching the local web service on \(backend.webURL.absoluteString)",
                webVisible: false
            )
        case .stopped:
            windowController?.setStatus("Tater is stopped", detail: "Use the menu bar item to start it again.", webVisible: false)
        case .failed(let message):
            windowController?.setStatus("Tater needs attention", detail: message, webVisible: false)
        }
    }

    @objc private func openTater() {
        showWindow()
    }

    @objc private func openBrowser() {
        NSWorkspace.shared.open(backend.webURL)
    }

    @objc private func toggleMascot() {
        mascotEnabled.toggle()
        UserDefaults.standard.set(mascotEnabled, forKey: mascotEnabledKey)
        mascotController?.setEnabled(mascotEnabled)
        refreshMascotMenu()
    }

    @objc private func startTater() {
        guard !migrationInProgress else { return }
        backend.start()
    }

    @objc private func stopTater() {
        guard !migrationInProgress else { return }
        backend.stop()
    }

    @objc private func restartTater() {
        guard !migrationInProgress else { return }
        backend.restart()
    }

    @objc private func showLogs() {
        backend.openLogsFolder()
    }

    @objc private func moveTaterData() {
        guard !migrationInProgress else { return }

        let panel = NSOpenPanel()
        panel.title = "Choose a New Location for Tater Data"
        panel.message = "Tater will create a “Tater Data” folder here. Choose a fast drive with enough room for your models."
        panel.prompt = "Choose Location"
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.canCreateDirectories = true
        panel.allowsMultipleSelection = false
        panel.directoryURL = backend.agentRoot.deletingLastPathComponent()
        guard panel.runModal() == .OK, let selectedParent = panel.url?.standardizedFileURL else { return }

        let source = backend.agentRoot.standardizedFileURL
        let destination = selectedParent.appendingPathComponent("Tater Data", isDirectory: true).standardizedFileURL
        guard source != destination else {
            presentMigrationAlert(title: "That is already Tater's data location", detail: source.path)
            return
        }
        let sourcePrefix = source.path.hasSuffix("/") ? source.path : source.path + "/"
        let destinationPrefix = destination.path.hasSuffix("/") ? destination.path : destination.path + "/"
        guard !destination.path.hasPrefix(sourcePrefix), !source.path.hasPrefix(destinationPrefix) else {
            presentMigrationAlert(title: "Choose a separate location", detail: "The new data folder cannot be inside the current data folder, or contain it.")
            return
        }
        guard !FileManager.default.fileExists(atPath: destination.path) else {
            presentMigrationAlert(title: "A Tater Data folder already exists there", detail: "Choose another location or move the existing folder out of the way first. Tater will not merge or overwrite data.")
            return
        }

        let confirm = NSAlert()
        confirm.messageText = "Move all Tater data?"
        confirm.informativeText = "Tater will stop, copy and verify your models, documents, workspace, and Redis data, switch to the new location, then start again. The old folder is moved to the Trash only after Tater is running from the new location."
        confirm.alertStyle = .informational
        confirm.addButton(withTitle: "Move Data")
        confirm.addButton(withTitle: "Cancel")
        guard confirm.runModal() == .alertFirstButtonReturn else { return }

        beginDataMigration(from: source, to: destination, selectedParent: selectedParent)
    }

    private func beginDataMigration(from source: URL, to destination: URL, selectedParent: URL) {
        migrationInProgress = true
        dataMigrationMenuItem?.isEnabled = false
        let cancellation = DataMigrationCancellation()
        migrationCancellation = cancellation
        let controller = DataMigrationWindowController(source: source, destination: destination)
        controller.onCancel = { cancellation.cancel() }
        migrationWindowController = controller
        controller.showWindow(nil)
        NSApp.activate(ignoringOtherApps: true)
        controller.setScanning()

        let previousBookmark = UserDefaults.standard.data(forKey: taterDataRootBookmarkKey)
        let scoped = selectedParent.startAccessingSecurityScopedResource() ? selectedParent : nil
        let newBookmark = try? selectedParent.bookmarkData(
            options: [.withSecurityScope],
            includingResourceValuesForKeys: nil,
            relativeTo: nil
        )

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self else { return }
            do {
                self.backend.stop(waitForExit: true)
                let stats = try self.migrationStats(at: source)
                try self.requireMigrationCapacity(stats.byteCount, at: selectedParent)
                try self.copyDataForMigration(
                    from: source,
                    to: destination,
                    stats: stats,
                    cancellation: cancellation
                )
                if cancellation.isCancelled { throw DataMigrationCancelled() }

                DispatchQueue.main.async {
                    controller.setSwitching("Verifying the new location")
                }
                let copiedStats = try self.migrationStats(at: destination)
                guard
                    copiedStats.entryCount == stats.entryCount,
                    copiedStats.fileCount == stats.fileCount,
                    copiedStats.byteCount == stats.byteCount
                else {
                    throw LauncherError("The copied data did not match the original. Tater kept using the original folder.")
                }

                self.backend.activateDataRoot(destination, securityScope: scoped, bookmark: newBookmark)
                DispatchQueue.main.async {
                    controller.setSwitching("Starting Tater from the new location")
                }
                self.backend.start()
                let startupResult = self.waitForBackendStart(timeout: 180)
                guard startupResult.ok else {
                    self.backend.stop(waitForExit: true)
                    self.backend.restoreDataRoot(source, bookmark: previousBookmark)
                    self.backend.start()
                    throw LauncherError("Tater could not start from the new location. It switched back to the original data folder. \(startupResult.message)")
                }

                var cleanupDetail = "The original data was moved to the Trash and can be recovered from there."
                do {
                    var trashedURL: NSURL?
                    try FileManager.default.trashItem(at: source, resultingItemURL: &trashedURL)
                } catch {
                    cleanupDetail = "Tater is using the new location. The original folder remains at \(source.path) because macOS could not move it to the Trash."
                }
                DispatchQueue.main.async {
                    self.finishDataMigration()
                    controller.finish(success: true, title: "Tater is running from \(destination.path)", detail: cleanupDetail)
                }
            } catch {
                if FileManager.default.fileExists(atPath: destination.path), self.backend.agentRoot.standardizedFileURL != destination.standardizedFileURL {
                    try? FileManager.default.removeItem(at: destination)
                }
                if self.backend.state == .stopped {
                    self.backend.start()
                }
                scoped?.stopAccessingSecurityScopedResource()
                DispatchQueue.main.async {
                    self.finishDataMigration()
                    controller.finish(success: false, title: error.localizedDescription, detail: "Your original data remains at \(source.path).")
                }
            }
        }
    }

    private func migrationStats(at root: URL) throws -> DataMigrationStats {
        guard FileManager.default.fileExists(atPath: root.path) else {
            return DataMigrationStats(entryCount: 0, fileCount: 0, byteCount: 0)
        }
        let keys: [URLResourceKey] = [.isRegularFileKey, .fileSizeKey]
        var enumerationError: Error?
        guard let enumerator = FileManager.default.enumerator(
            at: root,
            includingPropertiesForKeys: keys,
            options: [],
            errorHandler: { _, error in
                enumerationError = error
                return false
            }
        ) else {
            throw LauncherError("Tater could not read the current data folder.")
        }
        var entries = 0
        var files = 0
        var bytes: Int64 = 0
        for case let file as URL in enumerator {
            entries += 1
            let values = try file.resourceValues(forKeys: Set(keys))
            if values.isRegularFile == true {
                files += 1
                bytes += Int64(values.fileSize ?? 0)
            }
        }
        if let enumerationError { throw enumerationError }
        return DataMigrationStats(entryCount: entries, fileCount: files, byteCount: bytes)
    }

    private func requireMigrationCapacity(_ requiredBytes: Int64, at location: URL) throws {
        let values = try location.resourceValues(forKeys: [.volumeAvailableCapacityForImportantUsageKey])
        if let available = values.volumeAvailableCapacityForImportantUsage,
           available < requiredBytes + 1_073_741_824 {
            let formatter = ByteCountFormatter()
            formatter.countStyle = .file
            throw LauncherError(
                "The selected drive needs at least \(formatter.string(fromByteCount: requiredBytes + 1_073_741_824)) free before Tater can move the data."
            )
        }
    }

    private func copyDataForMigration(
        from source: URL,
        to destination: URL,
        stats: DataMigrationStats,
        cancellation: DataMigrationCancellation
    ) throws {
        let fm = FileManager.default
        try fm.createDirectory(at: destination, withIntermediateDirectories: false)
        let keys: [URLResourceKey] = [.isDirectoryKey, .isRegularFileKey, .isSymbolicLinkKey, .fileSizeKey]
        var enumerationError: Error?
        guard let enumerator = fm.enumerator(
            at: source,
            includingPropertiesForKeys: keys,
            options: [],
            errorHandler: { _, error in
                enumerationError = error
                return false
            }
        ) else {
            throw LauncherError("Tater could not enumerate the current data folder.")
        }

        var copiedBytes: Int64 = 0
        var copiedFiles = 0
        var lastUpdate = Date.distantPast
        for case let item as URL in enumerator {
            if cancellation.isCancelled { throw DataMigrationCancelled() }
            let components = item.pathComponents.dropFirst(source.pathComponents.count)
            let target = components.reduce(destination) { partial, component in
                partial.appendingPathComponent(component)
            }
            let values = try item.resourceValues(forKeys: Set(keys))
            if values.isDirectory == true && values.isSymbolicLink != true {
                try fm.createDirectory(at: target, withIntermediateDirectories: true)
                continue
            }
            try fm.createDirectory(at: target.deletingLastPathComponent(), withIntermediateDirectories: true)
            try fm.copyItem(at: item, to: target)
            if values.isRegularFile == true {
                copiedBytes += Int64(values.fileSize ?? 0)
                copiedFiles += 1
            }
            if Date().timeIntervalSince(lastUpdate) >= 0.08 || copiedFiles == stats.fileCount {
                lastUpdate = Date()
                DispatchQueue.main.async {
                    self.migrationWindowController?.setCopying(
                        copied: copiedBytes,
                        total: stats.byteCount,
                        files: copiedFiles,
                        totalFiles: stats.fileCount
                    )
                }
            }
        }
        if let enumerationError { throw enumerationError }
    }

    private func waitForBackendStart(timeout: TimeInterval) -> (ok: Bool, message: String) {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            switch backend.state {
            case .running:
                return (true, "")
            case .failed(let message):
                return (false, message)
            case .stopped, .bootstrapping, .starting:
                Thread.sleep(forTimeInterval: 0.4)
            }
        }
        return (false, "Startup timed out.")
    }

    private func finishDataMigration() {
        migrationInProgress = false
        migrationCancellation = nil
        refreshMenu(for: backend.state)
    }

    private func presentMigrationAlert(title: String, detail: String) {
        let alert = NSAlert()
        alert.messageText = title
        alert.informativeText = detail
        alert.alertStyle = .warning
        alert.addButton(withTitle: "OK")
        alert.runModal()
    }

    @objc private func checkForUpdates() {
        updater.checkForUpdates(manual: true)
    }

    @objc private func installUpdate() {
        updater.installAvailableUpdate()
    }

    @objc private func quit() {
        NSApp.terminate(nil)
    }

    private func showWindow() {
        if windowController == nil {
            windowController = TaterWindowController()
            windowController?.updateSetupProgress(from: backend.recentLogText())
        }
        windowController?.showWindow(nil)
        NSApp.activate(ignoringOtherApps: true)
        refreshWindow(for: backend.state)
    }
}

private struct LauncherError: LocalizedError {
    let message: String

    init(_ message: String) {
        self.message = message
    }

    var errorDescription: String? {
        message
    }
}

private let app = NSApplication.shared
private let delegate = AppDelegate()
app.delegate = delegate
app.run()
