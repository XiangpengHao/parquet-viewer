{
  description = "Parquet Viewer Flake Configuration";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { nixpkgs
    , rust-overlay
    , flake-utils
    , ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
      in
      {
        devShells.default = with pkgs;
          mkShell {
            buildInputs = [
              openssl
              pkg-config
              eza
              fd
              trunk
              wasm-pack
              wabt
              libiconv
              llvmPackages_19.clang-unwrapped
              llvmPackages_19.libcxx
              (rust-bin.fromRustupToolchainFile (./rust-toolchain.toml))
            ];
            shellHook = ''
              export CC=${pkgs.llvmPackages_19.clang-unwrapped}/bin/clang
              export C_INCLUDE_PATH="${pkgs.llvmPackages_19.libcxx.dev}/include/c++/v1:${pkgs.llvmPackages_19.clang-unwrapped.lib}/lib/clang/19/include"
              export CPLUS_INCLUDE_PATH="${pkgs.llvmPackages_19.libcxx.dev}/include/c++/v1:${pkgs.llvmPackages_19.clang-unwrapped.lib}/lib/clang/19/include"
            '';
          };
      }
    );
}