#!/bin/sh

set -e

go install .
maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --concurrency 2n
