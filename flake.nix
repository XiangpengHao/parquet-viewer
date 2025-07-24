{
  description = "Parquet Viewer Flake Configuration";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        rustPlatform = pkgs.makeRustPlatform {
          cargo = pkgs.rust-bin.selectLatestNightlyWith (toolchain:
            toolchain.default.override {
              targets = [ "wasm32-unknown-unknown" ];
            });
          rustc = pkgs.rust-bin.selectLatestNightlyWith (toolchain:
            toolchain.default.override {
              targets = [ "wasm32-unknown-unknown" ];
            });
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
            chromedriver
            chromium
            llvmPackages_20.clang
            lld_20
            llvmPackages_20.libcxx
            glibc_multi
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
