#!/bin/sh

set -e

SCRIPT_DIR="$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )"
CHALLENGE="$(basename "$SCRIPT_DIR")"

cd "$SCRIPT_DIR"/..
go build -o bin/"$CHALLENGE" ./"$CHALLENGE"
maelstrom test -w unique-ids --bin bin/"$CHALLENGE" --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition --concurrency 2n
