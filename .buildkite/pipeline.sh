#!/bin/bash

set -e

pipeline=$(printf "
steps:
    - label: \":rust: Cargo test all features\"
      command: ./.buildkite/docker.sh --features=all

    - label: \":rust: Cargo test no default features\"
      command: ./.buildkite/docker.sh --no-default-features

    - label: \":rust: Cargo test only chrono\"
      command: ./.buildkite/docker.sh --no-default-features --features chrono

    - wait

    - label: \":rust: Publish Rustdoc\"
      command: ./.buildkite/publish_rustdoc.sh
      branches: master
")

echo "$pipeline" | buildkite-agent pipeline upload
