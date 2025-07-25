#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/5b ./5b
maelstrom test -w kafka --bin bin/5b --node-count 2 --concurrency 4n --time-limit 20 --rate 1000
