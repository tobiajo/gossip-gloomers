#!/bin/sh

set -e

cd "$( cd -- "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )/.."

go build -o bin/6x_event-sourcing ./6x_event-sourcing
maelstrom test -w txn-rw-register --bin bin/6x_event-sourcing --node-count 2 --concurrency 4n --time-limit 20 --rate 1000 --availability total --nemesis partition
