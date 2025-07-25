
package main

import (
    "errors"
    "log"
    utils "github.com/tobiajo/gossip-gloomers/utils"
    "strconv"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
    kv  *maelstrom.KV
    tso *utils.TSO
}

type transaction = []operation
type operation = [3]any

type cell struct {
    Ts    int     `json:"ts"`
    Data  *int    `json:"data,omitempty"`
    Lock  *string `json:"lock,omitempty"`
    Write *string `json:"write,omitempty"`
}

type Txn struct {
    Txn transaction `json:"txn"`
}

type TxnOk struct {
    Type string      `json:"type"`
    Txn  transaction `json:"txn"`
}

func main() {
    n := maelstrom.NewNode()
    kv := maelstrom.NewLinKV(n)
    tso := utils.NewLinTSO(n)
    s := server{
        kv:  kv,
        tso: tso,
    }

    utils.RegisterHandler(n, "txn", s.txnHandler)

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}

func (s *server) txnHandler(req Txn) (TxnOk, error) {
    startTs, err := s.tso.Get()
    if err != nil {
        return TxnOk{}, err
    }

    var primary *int
    result := transaction{}

    // Prewrite Phase
    for _, op := range req.Txn {
        name := op[0].(string)
        key := int(op[1].(float64))
        keyStr := strconv.Itoa(key)

        if primary == nil {
            primary = &key
        }

        switch name {
        case "r":
            value, err := read(s.kv, key, startTs)
            if err != nil {
                return TxnOk{}, err
            }
            result = append(result, operation{name, key, value})
        case "w":
            value := int(op[2].(float64))
            err := preWrite(s.kv, key, startTs, value, strconv.Itoa(*primary))
            if err != nil {
                return TxnOk{}, err
            }
            result = append(result, operation{name, key, value})
        }
    }

    // Commit Phase
    commitTs, err := s.tso.Get()
    if err != nil {
        return TxnOk{}, err
    }

    for _, op := range req.Txn {
        name := op[0].(string)
        if name != "w" {
            continue
        }

        key := int(op[1].(float64))
        keyStr := strconv.Itoa(key)

        cells, err := utils.ReadOrElse(s.kv, keyStr, []cell{})
        if err != nil {
            return TxnOk{}, err
        }

        newCells := []cell{}
        for _, c := range cells {
            if c.Lock != nil && *c.Lock == strconv.Itoa(*primary) {
                newCells = append(newCells, cell{
                    Ts:    commitTs,
                    Data:  c.Data,
                    Write: c.Lock,
                })
            } else {
                newCells = append(newCells, c)
            }
        }

        if err := utils.Write(s.kv, keyStr, newCells); err != nil {
            return TxnOk{}, err
        }
    }

    return TxnOk{Type: "txn_ok", Txn: result}, nil
}

func read(kv *maelstrom.KV, key int, ts int) (int, error) {
    keyStr := strconv.Itoa(key)
    cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
    if err != nil {
        return 0, err
    }

    var latest *cell
    for _, c := range cells {
        if c.Write != nil && c.Ts <= ts {
            if latest == nil || c.Ts > latest.Ts {
                latest = &c
            }
        }
    }

    if latest == nil || latest.Data == nil {
        return 0, errors.New("no visible version")
    }

    return *latest.Data, nil
}

func preWrite(kv *maelstrom.KV, key int, ts int, data int, primary string) error {
    keyStr := strconv.Itoa(key)
    cells, err := utils.ReadOrElse(kv, keyStr, []cell{})
    if err != nil {
        return err
    }

    for _, c := range cells {
        if c.Lock != nil || c.Ts >= ts {
            return errors.New("conflict")
        }
    }

    cells = append(cells, cell{
        Ts:   ts,
        Data: &data,
        Lock: &primary,
    })

    return utils.Write(kv, keyStr, cells)
}
