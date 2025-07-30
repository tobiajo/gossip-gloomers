#!/bin/sh

set -e

SCRIPT_DIR="$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )"
CHALLENGE="$(basename "$SCRIPT_DIR")"

cd "$SCRIPT_DIR"/..
go build -o bin/"$CHALLENGE" ./"$CHALLENGE"
maelstrom test -w broadcast --bin bin/"$CHALLENGE" --node-count 5 --time-limit 20 --rate 10 --nemesis partition --concurrency 2n
