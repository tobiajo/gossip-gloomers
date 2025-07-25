#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/5c ./5c
maelstrom test -w kafka --bin bin/5c --node-count 2 --concurrency 4n --time-limit 20 --rate 1000
