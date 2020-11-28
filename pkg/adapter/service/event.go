package adapter

import "sync"

type Event struct {
	Payload EventPayload `json:"payload"`
}

type EventPayload struct {
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
	Source EventSource            `json:"source"`
	Op     string                 `json:"op"`
}

type EventSource struct {
	Snapshot string `json:"snapshot"`
	Table    string `json:"table"`
}

var eventPool = sync.Pool{
	New: func() interface{} {
		return &Event{}
	},
}

func (es *EventSource) IsSnapshot() bool {
	if es.Snapshot != "false" {
		return true
	}

	return false
}
