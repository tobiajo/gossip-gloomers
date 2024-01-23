#!/bin/sh

set -e

go install .
maelstrom test -w echo --bin ~/go/bin/maelstrom-echo --node-count 1 --time-limit 10 --concurrency 2n
