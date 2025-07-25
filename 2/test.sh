#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/2 ./2
maelstrom test -w unique-ids --bin bin/2 --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition --concurrency 2n
