#!/bin/sh

set -e

go install .

maelstrom test -w txn-rw-register --bin ~/go/bin/maelstrom-txn --node-count 2 --concurrency 4n --time-limit 20 --rate 1000 --availability total --nemesis partition
