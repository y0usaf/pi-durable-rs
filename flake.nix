{
  description = "pi-durable: durable conversations, tasks and documents over a transactional store, in Rust";

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
      build =
        pkgs: features:
        pkgs.rustPlatform.buildRustPackage {
          pname = manifest.name;
          inherit (manifest) version;
          inherit src;
          cargoLock.lockFile = ./Cargo.lock;
          doCheck = false;
          buildPhase = ''
            runHook preBuild
            cargo build --release --lib --frozen --examples --features "${features}"
            runHook postBuild
          '';
          installPhase = ''
            runHook preInstall
            mkdir -p $out/lib $out/bin
            cp target/release/lib*.rlib $out/lib/
            find target/release/examples -maxdepth 1 -type f -executable -exec cp {} $out/bin/ \;
            runHook postInstall
          '';
        };
    in
    {
      packages = forAll (pkgs: {
        default = build pkgs "providers";
        storage-only = build pkgs "";
      });

      checks = forAll (
        pkgs:
        let
          system = pkgs.stdenv.hostPlatform.system;
        in
        {
          build = self.packages.${system}.default;
          storage-only = self.packages.${system}.storage-only;
          clippy = self.packages.${system}.default.overrideAttrs (old: {
            pname = "${manifest.name}-clippy";
            nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.clippy ];
            buildPhase = "cargo clippy --all-targets --frozen --features providers -- -D warnings";
            installPhase = "touch $out";
          });
          oauth-flow = pkgs.runCommand "${manifest.name}-oauth-flow" { } ''
            ${self.packages.${system}.default}/bin/oauth_flow
            touch $out
          '';
          provider-e2e = pkgs.runCommand "${manifest.name}-provider-e2e" { } ''
            ${self.packages.${system}.default}/bin/provider_e2e
            touch $out
          '';
          fmt = pkgs.runCommand "${manifest.name}-fmt" {
            nativeBuildInputs = [
              pkgs.cargo
              pkgs.rustfmt
            ];
          } "cd ${src} && cargo fmt --check && touch $out";
        }
      );

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
