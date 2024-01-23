#!/bin/sh

set -e

go install .
maelstrom test -w kafka --bin ~/go/bin/maelstrom-kafka --node-count 1 --concurrency 4n --time-limit 20 --rate 1000
