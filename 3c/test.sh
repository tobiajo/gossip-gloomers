#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/3c ./3c
maelstrom test -w broadcast --bin bin/3c --node-count 5 --time-limit 20 --rate 10 --nemesis partition --concurrency 2n
