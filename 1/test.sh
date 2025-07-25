#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/1 ./1
maelstrom test -w echo --bin bin/1 --node-count 1 --time-limit 10 --concurrency 2n
