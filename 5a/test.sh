#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/5a ./5a
maelstrom test -w kafka --bin bin/5a --node-count 1 --concurrency 4n --time-limit 20 --rate 1000
