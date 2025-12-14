{
  description = "Parquet Viewer Flake Configuration";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    dioxus.url = "github:DioxusLabs/dioxus/v0.7.1";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, crane, dioxus, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        craneLib = crane.mkLib pkgs;
        wasm-bindgen-cli = craneLib.buildPackage {
          version = "0.2.106";
          src = craneLib.downloadCargoPackage {
            name = "wasm-bindgen-cli";
            version = "0.2.106";
            source = "registry+https://github.com/rust-lang/crates.io-index";
            checksum = "sha256-W2un/Iw5MRazARFXbZiJ143wcJ/E6loIlrTVQ4DiSzY=";
          };
          doCheck = false;
          pname = "wasm-bindgen-cli";
        };
         # Fetch daisyUI bundle files
        daisyui-bundle = pkgs.fetchurl {
          url = "https://github.com/saadeghi/daisyui/releases/v5.5.13/download/daisyui.mjs";
          sha256 = "sha256-dH6epo+aSV+eeh3uQbxd7MkWlG+6hCaGaknQ4Bnljj4=";
        };
        daisyui-theme-bundle = pkgs.fetchurl {
          url = "https://github.com/saadeghi/daisyui/releases/v5.5.13/download/daisyui-theme.mjs";
          sha256 = "sha256-iiUODarjHRxAD+tyOPh95xhHJELC40oczt+dsDo86yE=";
        };
      in {
        packages.default = craneLib.buildPackage {
          name = "paquet-viewer";
          version = "0.1.22";
          cargoHash = "sha256-c+usWtW5cCsTGbQ5g17rSNlycbDky5rEYn/0aSED3FM=";
          hardeningDisable = [ "all" ];
          buildInputs = with pkgs; [
            openssl
            pkg-config
            eza
            fd
            trunk
            wasm-pack
            wabt
            leptosfmt
            nodejs
            typescript
            pnpm
            vsce
            geckodriver
            firefox
            llvmPackages_20.clang
            lld_20
            llvmPackages_20.libcxx
            glibc_multi
           
          ];
          src = ./.;
        };
        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.packages.${system}.default ];
          packages = [
            wasm-bindgen-cli
            dioxus.packages.${system}.dioxus-cli
            wasm-bindgen-cli
            pkgs.binaryen  
            pkgs.tailwindcss_4
            (pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
                extensions = [ "rust-src" "llvm-tools-preview" ];
                targets = [ "x86_64-unknown-linux-gnu" "wasm32-unknown-unknown" ];
              }))
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
