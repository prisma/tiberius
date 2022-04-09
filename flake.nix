{
  description = "A Microsoft SQL Server TDS client";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    flake-utils = {
      url = "github:numtide/flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
      in {
        nixpkgs.overlays = [ rust-overlay.overlay ];
        devShell = pkgs.mkShell {
          nativeBuildInputs = [ pkgs.bashInteractive ];
          buildInputs = with pkgs; [
            gettext
            openssl
            pkg-config
            clangStdenv
            llvmPackages.libclang.lib
            kerberos
            rust-bin.stable.latest.default
          ];
          shellHook = with pkgs; ''
            export LIBCLANG_PATH="${llvmPackages.libclang.lib}/lib";
          '';
        };
      });
}
