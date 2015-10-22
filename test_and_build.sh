#!/usr/bin/env bash

set -x
set -e

cd /sequins
export GOPATH=$(pwd)/Godeps/_workspace

# test!
go test ./...

# Build
go build

# ensure that we built a viable binary
./sequins --help 2>&1 | grep usage && echo 'build looks viable'

# Stage Artifacts
cp -a ./sequins /build/
cp -a henson/ /build/
tree /build
/usr/games/cowsay "BUILD SUCCESSFUL"
