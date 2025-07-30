package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func RegisterHandler[Req any, Res any](n *maelstrom.Node, typ string, handler func(Req) (Res, error)) {
	n.Handle(typ, func(msg maelstrom.Message) error {
		var req Req
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		res, err := handler(req)
		if err != nil {
			return err
		}

		resJson, err := asJson(res)
		if err != nil {
			return err
		}
		resJson["type"] = typ + "_ok"
		return n.Reply(msg, resJson)
	})
}

func RegisterAsyncHandler[Req any](n *maelstrom.Node, typ string, handler func(Req) error) {
	n.Handle(typ, func(msg maelstrom.Message) error {
		var req Req
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		if err := handler(req); err != nil {
			return err
		}
		return nil
	})
}

// Stubborn synchronous send, retries until successful response.
func Send[Req any, Res any](n *maelstrom.Node, typ string, dest string, req Req) (Res, error) {
	reqJson, err := asJson(req)
	if err != nil {
		return *new(Res), err
	}
	reqJson["type"] = typ

	msg, err := n.SyncRPC(context.Background(), dest, reqJson)
	for err != nil {
		log.Default().Println("Retrying...")
		time.Sleep(time.Second)
		msg, err = n.SyncRPC(context.Background(), dest, req)
	}

	var res Res
	if err := json.Unmarshal(msg.Body, &res); err != nil {
		return *new(Res), err
	}
	return res, nil
}

func SendAsync[Req any](n *maelstrom.Node, typ string, dest string, req Req) error {
	reqJson, err := asJson(req)
	if err != nil {
		return err
	}
	reqJson["type"] = typ

	if err := n.Send(dest, reqJson); err != nil {
		return err
	}
	return nil
}

func DeepCopy[T any](src T) (T, error) {
	var dst T
	bytes, err := json.Marshal(src)
	if err != nil {
		return dst, fmt.Errorf("deep copy marshal failed: %w", err)
	}
	if err := json.Unmarshal(bytes, &dst); err != nil {
		return dst, fmt.Errorf("deep copy unmarshal failed: %w", err)
	}
	return dst, nil
}

func asJson(v any) (map[string]any, error) {
	vJsonRaw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var vJson map[string]any
	if err := json.Unmarshal(vJsonRaw, &vJson); err != nil {
		return nil, err
	}
	return vJson, nil
}

func ReadOrElse[V any](kv *maelstrom.KV, key string, defaultValue V) (V, error) {
	var value V
	err := kv.ReadInto(context.Background(), key, &value)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return defaultValue, nil
		} else {
			return *new(V), err
		}
	}
	return value, nil
}
