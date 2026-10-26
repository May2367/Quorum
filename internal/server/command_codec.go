package server

import (
	"encoding/json"
	"errors"
)

var errForeignCommand = errors.New("foreign replicated command")

type serviceCommandEnvelope struct {
	Service string          `json:"service"`
	Payload json.RawMessage `json:"payload"`
}

func encodeServiceCommand(service string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(serviceCommandEnvelope{Service: service, Payload: raw})
}

func decodeServiceCommand(data []byte, service string, target any) (bool, error) {
	var env serviceCommandEnvelope
	if err := json.Unmarshal(data, &env); err == nil && env.Service != "" {
		if env.Service != service {
			return true, errForeignCommand
		}
		return true, json.Unmarshal(env.Payload, target)
	}
	return false, nil
}
