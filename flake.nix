{
  description = "pi-durable: durable conversations, tasks and documents over a transactional store, in Rust";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.pi = {
    url = "github:earendil-works/pi/f1b2e77f5b13b2a199b1052cb79c235451afe7d7";
    flake = false;
  };

  outputs =
    {
      self,
      nixpkgs,
      pi,
    }:
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
      npm =
        pkgs: name: version: hash:
        pkgs.fetchurl {
          url = "https://registry.npmjs.org/${name}/-/${name}-${version}.tgz";
          inherit hash;
        };
      oracle =
        pkgs:
        let
          deps = {
            typebox = npm pkgs "typebox" "1.3.27" "sha256-WiiLzT1o9EVn3oSxheBNIF731bU2AoRrw9sw/dIZ3aM=";
            diff = npm pkgs "diff" "8.0.4" "sha256-71EyetWCQ89TMyYhn166pQKSgL0Pr8wR98IxVmOVk9Y=";
            partial-json =
              npm pkgs "partial-json" "0.1.7"
                "sha256-MOr+7W/cJfssok80w3eW2fmPC/XhVkUrmKLDVnYvYyI=";
          };
        in
        pkgs.runCommand "pi-ts-oracle" { } ''
          mkdir -p $out/packages $out/node_modules/@earendil-works $out/parity
          for p in ai chord durable; do
            mkdir -p $out/packages/$p
            cp -r ${pi}/packages/$p/src ${pi}/packages/$p/package.json $out/packages/$p
          done
          chmod -R u+w $out
          sed -i -E 's#\./dist/(.*)\.js"#./src/\1.ts"#' $out/packages/*/package.json
          ln -s ../../packages/ai $out/node_modules/@earendil-works/pi-ai
          ln -s ../../packages/chord $out/node_modules/@earendil-works/chord
          ln -s ../../packages/durable $out/node_modules/@earendil-works/pi-durable
          mkdir $out/packages/ai/src/providers/data
          for f in $(grep -ho 'data/[a-z0-9-]*\.json' $out/packages/ai/src/providers/*.models.ts | sort -u) data/.manifest.json; do
            echo '{}' > $out/packages/ai/src/providers/$f
          done
          ${nixpkgs.lib.concatStrings (
            nixpkgs.lib.mapAttrsToList (name: tgz: ''
              mkdir $out/node_modules/${name}
              tar -xzf ${tgz} --strip-components=1 -C $out/node_modules/${name}
            '') deps
          )}
          cp ${./parity}/{driver.ts,register.mjs,loader.mjs} $out/parity/
        '';
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
          openai-responses-e2e = pkgs.runCommand "${manifest.name}-openai-responses-e2e" { } ''
            ${self.packages.${system}.default}/bin/openai_responses_e2e
            touch $out
          '';
          openai-completions-e2e = pkgs.runCommand "${manifest.name}-openai-completions-e2e" { } ''
            ${self.packages.${system}.default}/bin/openai_completions_e2e
            touch $out
          '';
          parity =
            pkgs.runCommand "${manifest.name}-parity"
              {
                nativeBuildInputs = [
                  pkgs.nodejs_24
                  pkgs.gnugrep
                ];
                PARITY_TS = "${oracle pkgs}/parity";
                PARITY_RS = "${self.packages.${system}.default}/bin/parity";
              }
              ''
                bash ${./parity}/run.sh
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
