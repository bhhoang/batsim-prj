{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=23.11";
    flake-utils.url = "github:numtide/flake-utils";
    nur-kapack = {
      url = "github:oar-team/nur-kapack/master";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
    intervalset-flake = {
      url = "git+https://framagit.org/batsim/intervalset";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.nur-kapack.follows = "nur-kapack";
      inputs.flake-utils.follows = "flake-utils";
    };
    batprotocol-flake = {
      url = "git+https://framagit.org/batsim/batprotocol";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.nur-kapack.follows = "nur-kapack";
      inputs.flake-utils.follows = "flake-utils";
    };
    batsim-flake = {
      url = "git+https://framagit.org/batsim/batsim?ref=batprotocol";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.nur-kapack.follows = "nur-kapack";
      inputs.batprotocol.follows = "batprotocol-flake";
      inputs.intervalset.follows = "intervalset-flake";
      inputs.flake-utils.follows = "flake-utils";
    };
  };

  outputs = { self, nixpkgs, nur-kapack, intervalset-flake, flake-utils, batprotocol-flake, batsim-flake }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        py = pkgs.python3;
        pyPkgs = pkgs.python3Packages;
        kapack = nur-kapack.packages.${system};
        batprotopkgs = batprotocol-flake.packages-debug.${system};
        intervalsetpkgs = intervalset-flake.packages-debug.${system};
        batpkgs = batsim-flake.packages-debug.${system};
      in rec {
        packages = rec {
          docker-container = pkgs.dockerTools.buildImage {
            name = "batsim-getting-started";
            tag = "latest";
            copyToRoot = devShells.default.buildInputs ++ [ pkgs.bashInteractive pkgs.coreutils pkgs.gcc ];
            config = {
              Cmd = [ "/bin/bash" ];
              Env = [
                "PKG_CONFIG_PATH=${batprotopkgs.batprotocol-cpp}/lib/pkgconfig:${intervalsetpkgs.intervalset}/lib/pkgconfig:${pkgs.nlohmann_json}/share/pkgconfig"
              ];
            };
          };
        };
        devShells = rec {
          default = pkgs.mkShell {
            buildInputs = with pkgs; [
              # program deps
              batpkgs.batsim

              # libraries deps
              batprotopkgs.batprotocol-cpp
              intervalsetpkgs.intervalset
              nlohmann_json

              # build deps
              meson ninja pkg-config

              # runtime deps
              gdb cgdb
              kapack.evalys
              python3
            ];

            DEBUG_SRC_DIRS = batpkgs.batsim.DEBUG_SRC_DIRS ++ batprotopkgs.batprotocol-cpp.DEBUG_SRC_DIRS ++ intervalsetpkgs.intervalset.DEBUG_SRC_DIRS;
            GDB_DIR_ARGS = batpkgs.batsim.GDB_DIR_ARGS ++ batprotopkgs.batprotocol-cpp.GDB_DIR_ARGS ++ intervalsetpkgs.intervalset.GDB_DIR_ARGS;

            hardeningDisable = [ "fortify" ];
            shellHook = ''
              echo '⚠️ DO NOT USE THIS SHELL FOR A REAL EXPERIMENT! ⚠️'
              echo 'This shell is meant to get started with batsim (batprotocol version)'
              echo 'All softwares have been compiled in debug mode, which is extremely slow'
              echo
              echo 'Add the following arguments to GDB to explore sources of the Batsim ecosystem freely.'
              echo gdb \$\{GDB_DIR_ARGS\}
            '';
          };
        };
      }
    );
}
