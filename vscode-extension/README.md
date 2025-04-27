# Parquet Viewer for VS Code

This extension allows you to view and query Apache Parquet files directly within Visual Studio Code. It integrates the web-based Parquet Viewer into VS Code.

## Features

- View Parquet files directly in VS Code
- Explore schema and metadata
- Execute SQL queries on Parquet data
- Export data to CSV

## Setup and Build Instructions

This extension is built on a Rust + WebAssembly Parquet viewer, with a TypeScript wrapper to integrate it with VS Code.

### Prerequisites

- Node.js 14+
- Rust and Cargo (for building the Parquet viewer)
- Trunk (for bundling the Rust WebAssembly application)

### Build Steps

1. First, build the Rust WebAssembly application:

```bash
# From the root directory
cargo install trunk  # If you don't have trunk installed
trunk build --release
```

This will create a `dist` directory with the compiled WebAssembly application.

2. Then, build the VS Code extension:

```bash
# From the vscode-extension directory
npm install
npm run compile
```

The compile script will:
- Compile the TypeScript code
- Copy the necessary files from the Rust build output to the extension's dist directory



