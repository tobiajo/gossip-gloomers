module maelstrom-broadcast

go 1.20

require (
	github.com/deckarep/golang-set/v2 v2.3.0
	github.com/jepsen-io/maelstrom/demo/go v0.0.0-20230623004240-10f5c7f61e0e
	maelstrom-utils v0.0.0-00010101000000-000000000000
)

replace maelstrom-utils => ../maelstrom-utils
