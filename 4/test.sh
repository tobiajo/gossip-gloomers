#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/4 ./4
maelstrom test -w g-counter --bin bin/4 --node-count 3 --rate 100 --time-limit 20 --nemesis partition --concurrency 2n
