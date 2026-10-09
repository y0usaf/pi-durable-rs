{
  description = "Pi-Rust-Durable: Rust reproduction of @earendil-works/pi-durable";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      manifest = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).package;
      src = nixpkgs.lib.cleanSource ./.;
    in
    {
      packages = forAll (pkgs: {
        default = pkgs.rustPlatform.buildRustPackage {
          pname = manifest.name;
          inherit (manifest) version;
          inherit src;
          cargoLock.lockFile = ./Cargo.lock;
          doCheck = false;
          buildPhase = ''
            runHook preBuild
            cargo build --release --lib --frozen
            runHook postBuild
          '';
          installPhase = ''
            runHook preInstall
            mkdir -p $out/lib
            cp target/release/lib*.rlib $out/lib/
            runHook postInstall
          '';
        };
      });

      checks = forAll (pkgs: {
        build = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
        clippy = self.packages.${pkgs.stdenv.hostPlatform.system}.default.overrideAttrs (old: {
          pname = "${manifest.name}-clippy";
          nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.clippy ];
          buildPhase = "cargo clippy --all-targets --frozen -- -D warnings";
          installPhase = "touch $out";
        });
        fmt = pkgs.runCommand "${manifest.name}-fmt" {
          nativeBuildInputs = [
            pkgs.cargo
            pkgs.rustfmt
          ];
        } "cd ${src} && cargo fmt --check && touch $out";
      });

      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.cargo
            pkgs.rustc
            pkgs.clippy
            pkgs.rustfmt
            pkgs.nodejs
          ];
        };
      });

      formatter = forAll (pkgs: pkgs.nixfmt);
    };
}
