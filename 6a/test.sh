#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/6a ./6a
maelstrom test -w txn-rw-register --bin bin/6a --node-count 1 --time-limit 20 --rate 1000 --concurrency 4n --availability total
