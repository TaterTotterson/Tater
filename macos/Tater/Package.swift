// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "TaterAssistant",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .executable(name: "TaterAssistant", targets: ["TaterAssistant"])
    ],
    targets: [
        .executableTarget(
            name: "TaterAssistant",
            path: "Sources/TaterAssistant",
            linkerSettings: [
                .linkedFramework("AppKit"),
                .linkedFramework("WebKit")
            ]
        )
    ]
)
