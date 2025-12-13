{
  description = "Parquet Viewer Flake Configuration";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    dioxus.url = "github:DioxusLabs/dioxus/v0.7.1";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, dioxus, crane, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        craneLib = crane.mkLib pkgs;
        rustPlatform = pkgs.makeRustPlatform {
          cargo = pkgs.rust-bin.nightly."2025-08-28".default.override {
              targets = [ "wasm32-unknown-unknown" ];
              extensions = [ "rust-src" ];
          };
          rustc = pkgs.rust-bin.nightly."2025-08-28".default.override {
            targets = [ "wasm32-unknown-unknown" ];
            extensions = [ "rust-src" ];
          };
        };
        wasm-bindgen-cli = craneLib.buildPackage {
          version = "0.2.105";
          src = craneLib.downloadCargoPackage {
            name = "wasm-bindgen-cli";
            version = "0.2.105";
            source = "registry+https://github.com/rust-lang/crates.io-index";
            checksum = "sha256-Dm323jfd6JPt71KlTvEnfeMTd44f4/G2eMFdmMk9OlA=";
          };
          doCheck = false;
          pname = "wasm-bindgen-cli";
        };
        
        # Fetch daisyUI bundle files
        daisyui-bundle = pkgs.fetchurl {
          url = "https://github.com/saadeghi/daisyui/releases/latest/download/daisyui.mjs";
          sha256 = "sha256-dH6epo+aSV+eeh3uQbxd7MkWlG+6hCaGaknQ4Bnljj4=";
        };
        daisyui-theme-bundle = pkgs.fetchurl {
          url = "https://github.com/saadeghi/daisyui/releases/latest/download/daisyui-theme.mjs";
          sha256 = "sha256-iiUODarjHRxAD+tyOPh95xhHJELC40oczt+dsDo86yE=";
        };
      in {
        packages.default = rustPlatform.buildRustPackage {
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
            tailwindcss_4
            dioxus.packages.${system}.dioxus-cli
            wasm-bindgen-cli
            binaryen
          ];
          src = ./.;
        };
        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.packages.${system}.default ];
          shellHook = ''
            unset NIX_HARDENING_ENABLE
            export CC=${pkgs.llvmPackages_20.clang}/bin/clang
          '';
        };
      });
}
