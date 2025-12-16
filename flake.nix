{
  description = "Parquet Viewer Flake Configuration";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    dioxus.url = "github:DioxusLabs/dioxus/v0.7.1";
  };

  nixConfig = {
    extra-substituters = [
      "https://nix-community.cachix.org"
      "https://crane.cachix.org"
    ];
    extra-trusted-public-keys = [
      "nix-community.cachix.org-1:mB9FSh9qf2dCimDSUo8Zy7bkq5CX+/rkCWyvRCYg3Fs="
      "crane.cachix.org-1:8Sw/sLLG+rE9xXFMfOW8qYh5FQwUwhK9j4gT3gqCfNY="
    ];
  };
  
  outputs = { self, nixpkgs, rust-overlay, flake-utils, crane, dioxus, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        craneLib = crane.mkLib pkgs;
        wasm-bindgen-cli = pkgs.stdenv.mkDerivation {
          pname = "wasm-bindgen-cli";
          version = "0.2.106";
          src = pkgs.fetchurl {
            url = "https://github.com/rustwasm/wasm-bindgen/releases/download/0.2.106/wasm-bindgen-0.2.106-x86_64-unknown-linux-musl.tar.gz";
            sha256 = "sha256-Pz564MCnRI/LAOn8KER34DFawPH/gyjzL2v5rH/ksCw=";
          };
          nativeBuildInputs = [ pkgs.autoPatchelfHook ];
          installPhase = ''
            mkdir -p $out/bin
            cp wasm-bindgen wasm-bindgen-test-runner wasm2es6js $out/bin/
          '';
        };
         # Fetch daisyUI bundle files
        daisyui-bundle = pkgs.fetchurl {
          url = "https://github.com/saadeghi/daisyui/releases/download/v5.5.14/daisyui.mjs";
          sha256 = "sha256-ZhCaZQYZiADXoO3UwaAqv3cxiYu87LEiZuonefopRUw=";
        };
        daisyui-theme-bundle = pkgs.fetchurl {
          url = "https://github.com/saadeghi/daisyui/releases/download/v5.5.14/daisyui-theme.mjs";
          sha256 = "sha256-PPO2fLQ7eB+ROYnpmK5q2LHIoWUE+EcxYmvjC+gzgSw=";
        };

        # Filter source to only include files relevant to Rust builds
        src = craneLib.cleanCargoSource ./.;

        # Common arguments shared between dependency and main builds
        commonArgs = {
          inherit src;
          strictDeps = true;
          hardeningDisable = [ "all" ];

          # Build-time tools
          nativeBuildInputs = with pkgs; [
            pkg-config
            llvmPackages_20.clang
            lld_20
            wasm-pack
          ];

          # Runtime/link-time libraries
          buildInputs = with pkgs; [
            openssl
          ];
        };

        # Build dependencies only (cached separately from source code)
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

      in {
        packages.default = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          pname = "paquet-viewer";
          version = "0.1.22";
        });
        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.packages.${system}.default ];
          packages = [
            # Rust and WASM tooling
            wasm-bindgen-cli
            dioxus.packages.${system}.dioxus-cli
            pkgs.binaryen
            pkgs.wabt
            pkgs.tailwindcss_4
            (pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
                extensions = [ "rust-src" "llvm-tools-preview" ];
                targets = [ "x86_64-unknown-linux-gnu" "wasm32-unknown-unknown" ];
              }))

            # Dev utilities
            pkgs.eza
            pkgs.fd

            # JavaScript/TypeScript tooling
            pkgs.nodejs
            pkgs.typescript
            pkgs.pnpm

            # VSCode extension tooling
            pkgs.vsce

            # Browser testing tools
            pkgs.geckodriver
            pkgs.firefox
          ];
          shellHook = ''
            unset NIX_HARDENING_ENABLE
            export CC=${pkgs.llvmPackages_20.clang}/bin/clang
            
            # Setup daisyUI vendor files 
            VENDOR_DIR="vendor"
            mkdir -p "$VENDOR_DIR"
              
            # Copy daisyUI files from Nix store if they don't exist or are outdated
            if [ ! -f "$VENDOR_DIR/daisyui.mjs" ] || [ "${daisyui-bundle}" -nt "$VENDOR_DIR/daisyui.mjs" ]; then
              echo "Setting up daisyUI bundle files..."
              cp -f "${daisyui-bundle}" "$VENDOR_DIR/daisyui.mjs"
              cp -f "${daisyui-theme-bundle}" "$VENDOR_DIR/daisyui-theme.mjs"
              echo "daisyUI files ready in $VENDOR_DIR"
            fi
          '';
        };
      });
}
