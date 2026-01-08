{
  description = "Parquet Viewer Flake Configuration";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      flake-utils,
      crane,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        rustToolchain = pkgs.rust-bin.selectLatestNightlyWith (
          toolchain:
          toolchain.default.override {
            extensions = [
              "rust-src"
              "llvm-tools-preview"
            ];
            targets = [ "wasm32-unknown-unknown" ];
          }
        );
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
        version = cargoToml.package.version;
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
          url = "https://github.com/saadeghi/daisyui/releases/download/v5.5.14/daisyui.mjs";
          sha256 = "sha256-ZhCaZQYZiADXoO3UwaAqv3cxiYu87LEiZuonefopRUw=";
        };
        daisyui-theme-bundle = pkgs.fetchurl {
          url = "https://github.com/saadeghi/daisyui/releases/download/v5.5.14/daisyui-theme.mjs";
          sha256 = "sha256-PPO2fLQ7eB+ROYnpmK5q2LHIoWUE+EcxYmvjC+gzgSw=";
        };

        src = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter =
            path: type:
            (craneLib.filterCargoSources path type)
            || (builtins.match ".*/assets/.*" path != null)
            || (builtins.match ".*/Dioxus.toml$" path != null)
            || (builtins.match ".*/tailwind.css$" path != null);
        };

        commonEnv = {
          inherit src version;
          pname = "parquet-viewer";
          strictDeps = true;
          CARGO_BUILD_TARGET = "wasm32-unknown-unknown";
          nativeBuildInputs = [
            pkgs.pkg-config
            pkgs.llvmPackages_20.clang-unwrapped
            pkgs.lld_20
          ];
          buildInputs = with pkgs; [
            openssl
          ];
        };

        cargoArtifacts = craneLib.buildDepsOnly (
          commonEnv
          // {
            cargoHash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
          }
        );

        # CLI-specific setup (builds for native target, not wasm)
        cliSrc = pkgs.lib.cleanSourceWith {
          src = ./cli;
          filter = path: type: craneLib.filterCargoSources path type;
        };

        cliCargoArtifacts = craneLib.buildDepsOnly {
          src = cliSrc;
          pname = "parquet-viewer-cli";
          version = "0.1.0";
          strictDeps = true;
        };
      in
      {
        packages.web = craneLib.mkCargoDerivation (
          commonEnv
          // {
            pname = "parquet-viewer-web";
            inherit cargoArtifacts;
            doInstallCargoArtifacts = false; # Don't include target.tar.zst in output
            nativeBuildInputs = [
              pkgs.pkg-config
              pkgs.llvmPackages_20.clang-unwrapped
              pkgs.lld_20
              pkgs.dioxus-cli
              wasm-bindgen-cli
              pkgs.binaryen
              pkgs.wabt
              pkgs.wasm-pack
            ];
            buildInputs = with pkgs; [ openssl ];

            buildPhaseCargoCommand = ''
              export HOME="$TMPDIR/home"
              mkdir -p "$HOME"
              # Target-specific CC for the cc crate (hyphens become underscores)
              export CC_wasm32_unknown_unknown=${pkgs.llvmPackages_20.clang-unwrapped}/bin/clang
              export CFLAGS_wasm32_unknown_unknown="-isystem ${pkgs.llvmPackages_20.clang-unwrapped.lib}/lib/clang/20/include"

              # Setup daisyUI vendor files for tailwind
              mkdir -p vendor
              cp ${daisyui-bundle} vendor/daisyui.mjs
              cp ${daisyui-theme-bundle} vendor/daisyui-theme.mjs

              # Generate Tailwind CSS (source file is tailwind.css at root)
              ${pkgs.tailwindcss_4}/bin/tailwindcss -i tailwind.css -o assets/tailwind.css

              export CARGO_NET_OFFLINE=true
              export DX_LOG=info
              dx bundle --platform web --release
            '';

            installPhaseCommand = ''
              mkdir -p "$out"
              cp -r target/dx/parquet-viewer/release/web/public/* "$out/"
            '';
          }
        );

        packages.vscode-extension = pkgs.buildNpmPackage {
          pname = "parquet-viewer-vscode-extension";
          inherit version;

          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              (pkgs.lib.hasInfix "/vscode-extension" path)
              || (pkgs.lib.hasInfix "/assets" path)
              || (builtins.baseNameOf path == "README.md")
              || (builtins.baseNameOf path == "LICENSE-APACHE")
              || (builtins.baseNameOf path == "LICENSE-MIT");
          };
          sourceRoot = "source/vscode-extension";
          npmDepsHash = "sha256-e904TJ6sIIuNScRRzb/xzhgd76A1INDcl8m57qXcktM=";

          nativeBuildInputs = with pkgs; [
            nodejs
            vsce
            typescript
          ];

          postPatch = ''
            # Copy web build output
            mkdir -p dist/assets
            cp -r ${self.packages.${system}.web}/* dist/

            # Copy icon
            cp ../assets/icon-192x192.png dist/assets/icon-192x192.png

            # Replace LICENSE symlink with actual file
            rm -f LICENSE
            cp ../LICENSE-APACHE LICENSE
          '';

          buildPhase = ''
            runHook preBuild

            # Compile TypeScript
            npm run compile

            # Package extension
            vsce package --out parquet-querier-${version}.vsix

            runHook postBuild
          '';

          installPhase = ''
            mkdir -p $out
            cp parquet-querier-${version}.vsix $out/
          '';
        };

        packages.cli = craneLib.buildPackage {
          src = cliSrc;
          pname = "parquet-viewer-cli";
          version = "0.1.0";
          cargoArtifacts = cliCargoArtifacts;
          strictDeps = true;

          meta = {
            description = "CLI to serve local parquet files and open them in parquet-viewer";
            mainProgram = "parquet-viewer-cli";
          };
        };

        packages.docker = pkgs.dockerTools.buildLayeredImage {
          name = "parquet-viewer";
          tag = version;

          contents = [
            pkgs.nginx
            pkgs.fakeNss
          ];

          extraCommands = ''
            # Create nginx directories
            mkdir -p tmp/nginx_client_body
            mkdir -p var/log/nginx
            mkdir -p var/cache/nginx
            mkdir -p etc/nginx

            # Copy web files to nginx html directory
            mkdir -p usr/share/nginx/html
            cp -r ${self.packages.${system}.web}/* usr/share/nginx/html/
          '';

          config = {
            Cmd = [
              "${pkgs.nginx}/bin/nginx"
              "-g"
              "daemon off;"
            ];
            ExposedPorts = {
              "80/tcp" = { };
            };
            WorkingDir = "/usr/share/nginx/html";
          };
        };

        packages.default = self.packages.${system}.web;
        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.packages.${system}.web ];
          packages = [
            wasm-bindgen-cli
            pkgs.dioxus-cli
            pkgs.binaryen
            pkgs.tailwindcss_4
            rustToolchain
            pkgs.vsce
            pkgs.nixd
          ];
          shellHook = ''
            # Setup clang for wasm32 cross-compilation
            export CC_wasm32_unknown_unknown=${pkgs.llvmPackages_20.clang-unwrapped}/bin/clang
            export CFLAGS_wasm32_unknown_unknown="-isystem ${pkgs.llvmPackages_20.clang-unwrapped.lib}/lib/clang/20/include"
            export CC=${pkgs.llvmPackages_20.clang-unwrapped}/bin/clang
            export CFLAGS="-isystem ${pkgs.llvmPackages_20.clang-unwrapped.lib}/lib/clang/20/include"
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
      }
    );
}
