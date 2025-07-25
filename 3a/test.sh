#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/3a ./3a
maelstrom test -w broadcast --bin bin/3a --node-count 1 --time-limit 20 --rate 10 --concurrency 2n
