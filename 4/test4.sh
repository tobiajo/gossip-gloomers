#!/bin/sh

set -e

go install .
maelstrom test -w g-counter --bin ~/go/bin/maelstrom-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition --concurrency 2n
