// Package observe converts public Forest diagnostics into detached, safe data.
package observe

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidCursor = errors.New("invalid observation cursor")

type Cursor struct {
	BootID   string
	Sequence uint64
}

func (c Cursor) String() string { return c.BootID + ":" + strconv.FormatUint(c.Sequence, 10) }

func ParseCursor(value string) (*Cursor, error) {
	boot, seq, ok := strings.Cut(value, ":")
	if !ok || len(boot) != 32 || len(seq) == 0 || len(seq) > 20 {
		return nil, ErrInvalidCursor
	}
	for _, ch := range boot {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return nil, ErrInvalidCursor
		}
	}
	for _, ch := range seq {
		if ch < '0' || ch > '9' {
			return nil, ErrInvalidCursor
		}
	}
	n, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	return &Cursor{BootID: boot, Sequence: n}, nil
}

type Frame struct {
	SchemaVersion int       `json:"schema_version"`
	BootID        string    `json:"boot_id"`
	SampledAt     time.Time `json:"sampled_at"`
	Snapshot      Snapshot  `json:"snapshot"`
	Events        []Change  `json:"events"`
	Cursor        string    `json:"cursor"`
	ResetReason   string    `json:"reset_reason,omitempty"`
}

type Diagnostic struct {
	Code  string `json:"code"`
	Stage string `json:"stage,omitempty"`
	Tree  string `json:"tree,omitempty"`
	Port  string `json:"port,omitempty"`
}

type Snapshot struct {
	Generation            uint64     `json:"generation,string"`
	ObservedEventSequence uint64     `json:"observed_event_sequence,string"`
	Trees                 []Tree     `json:"trees"`
	NestEdges             []NestEdge `json:"nest_edges"`
	Bindings              []Binding  `json:"bindings"`
	Instances             []Instance `json:"instances"`
	Counts                Counts     `json:"counts"`
}

type Counts struct {
	Trees        uint64 `json:"trees,string"`
	NestEdges    uint64 `json:"nest_edges,string"`
	Links        uint64 `json:"links,string"`
	NestBindings uint64 `json:"nest_bindings,string"`
	Draining     uint64 `json:"draining,string"`
}

type Tree struct {
	ID           string       `json:"id"`
	ParentID     string       `json:"parent_id"`
	InstanceID   string       `json:"instance_id"`
	TypeID       string       `json:"type_id"`
	Version      string       `json:"version"`
	SampledState string       `json:"sampled_state"`
	Ports        []Port       `json:"ports"`
	Capabilities []Capability `json:"capabilities"`
}

type Port struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	Required   bool   `json:"required"`
	Many       bool   `json:"many"`
	Phase      string `json:"phase"`
	ContractID string `json:"contract_id"`
	MinVersion uint64 `json:"min_version,string"`
	MaxVersion uint64 `json:"max_version,string"`
}

type Capability struct {
	ID              string   `json:"id"`
	Type            string   `json:"type"`
	Visibility      []string `json:"visibility"`
	ContractID      string   `json:"contract_id"`
	ContractVersion uint64   `json:"contract_version,string"`
}

type NestEdge struct {
	Parent string `json:"parent"`
	Child  string `json:"child"`
}

type Binding struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Consumer   string `json:"consumer"`
	Port       string `json:"port"`
	Provider   string `json:"provider"`
	Capability string `json:"capability"`
	Order      uint64 `json:"order,string"`
}

type Instance struct {
	ID         string      `json:"id"`
	TreeID     string      `json:"tree_id"`
	TypeID     string      `json:"type_id"`
	Version    string      `json:"version"`
	State      string      `json:"state"`
	Diagnostic *Diagnostic `json:"diagnostic,omitempty"`
}

type Change struct {
	Sequence   uint64      `json:"sequence,string"`
	Generation uint64      `json:"generation,string"`
	Operations []string    `json:"operations"`
	Trees      []string    `json:"trees"`
	Links      []string    `json:"links"`
	Result     string      `json:"result"`
	Timestamp  time.Time   `json:"timestamp"`
	Diagnostic *Diagnostic `json:"diagnostic,omitempty"`
}
