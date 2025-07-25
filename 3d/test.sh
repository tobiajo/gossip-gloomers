#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/3d ./3d
maelstrom test -w broadcast --bin bin/3d --node-count 25 --time-limit 20 --rate 100 --latency 100 --concurrency 2n
