#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/3b ./3b
maelstrom test -w broadcast --bin bin/3b --node-count 5 --time-limit 20 --rate 10 --concurrency 2n
