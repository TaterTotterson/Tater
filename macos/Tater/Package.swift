// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "TaterAssistant",
    platforms: [
        .macOS("15.0")
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
