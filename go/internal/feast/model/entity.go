package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Entity struct {
	Name     string
	JoinKey  string                          // DEPRECATED: Use JoinKeys instead
	JoinKeys map[string]types.ValueType_Enum // New field for multiple join keys (key: join_key_name, value: value_type)
}

func NewEntityFromProto(proto *core.Entity) *Entity {
	entity := &Entity{
		Name:     proto.Spec.Name,
		JoinKey:  proto.Spec.JoinKey, // Backward compatibility
		JoinKeys: make(map[string]types.ValueType_Enum),
	}

	// Check if the new join_keys format is available
	if len(proto.Spec.JoinKeys) > 0 {
		// New format: use join_keys map with value types
		for joinKeyName, joinKeySpec := range proto.Spec.JoinKeys {
			entity.JoinKeys[joinKeyName] = joinKeySpec.ValueType
		}
	} else {
		// Legacy format: use single join_key
		entity.JoinKeys[proto.Spec.JoinKey] = proto.Spec.ValueType
	}

	return entity
}
