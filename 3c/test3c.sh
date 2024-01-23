#!/bin/sh

set -e

go install .
maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition --concurrency 2n
