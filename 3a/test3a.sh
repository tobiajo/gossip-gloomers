#!/bin/sh

set -e

go install .
maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 1 --time-limit 20 --rate 10 --concurrency 2n
