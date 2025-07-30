#!/bin/sh

set -e

SCRIPT_DIR="$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )"
CHALLENGE="$(basename "$SCRIPT_DIR")"

cd "$SCRIPT_DIR"/..
go build -o bin/"$CHALLENGE" ./"$CHALLENGE"
maelstrom test -w echo --bin bin/"$CHALLENGE" --node-count 1 --time-limit 10 --concurrency 2n
