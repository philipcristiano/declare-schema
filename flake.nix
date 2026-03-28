{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };
  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          rustToolchain = pkgs.rust-bin.stable.latest.default;
          package_version = "0.0.12";
          package_name = "declare-schema";

          linuxPkgs = import nixpkgs {
            system = "x86_64-linux";
            overlays = overlays;
          };
          package = pkgs.rustPlatform.buildRustPackage {
            pname = package_name;
            version = package_version;
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;

            nativeBuildInputs = [ rustToolchain ];
          };
                    # Build the package targeting linux for the Docker image
          linuxPackage = linuxPkgs.rustPlatform.buildRustPackage {
            pname = package_name;
            version = package_version;
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = [ linuxPkgs.rust-bin.stable.latest.default ];
          };
        in
        with pkgs;
        {
          devShells.default = mkShell {
            buildInputs = [
                rust-bin.stable.latest.default
            ];
          };
          packages.default = package;
          packages.docker = linuxPkgs.dockerTools.buildLayeredImage {
            name = package_name;
            tag = package_version;
            contents = [ linuxPackage ];
            config = {
              Cmd = [ "/bin/declare-schema" ];
            };
          };
        }
      );
}
