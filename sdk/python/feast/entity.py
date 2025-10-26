# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Union

from google.protobuf.json_format import MessageToJson
from typeguard import typechecked

from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.Entity_pb2 import EntityMeta as EntityMetaProto
from feast.protos.feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
from feast.value_type import ValueType


@typechecked
class Entity:
    """
    An entity defines a collection of entities for which features can be defined. An
    entity can also contain associated metadata.

    Attributes:
        name: The unique name of the entity.
        value_type: The type of the entity, such as string or float.
        join_key: A property that uniquely identifies different entities within the
            collection. The join_key property is typically used for joining entities
            with their associated features. If not specified, defaults to the name.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the entity, typically the email of the primary maintainer.
        created_timestamp: The time when the entity was created.
        last_updated_timestamp: The time when the entity was last updated.
    """

    name: str
    value_type: ValueType
    join_key: str
    join_keys: Dict[str, ValueType]  # New field for multiple join keys
    description: str
    tags: Dict[str, str]
    owner: str
    created_timestamp: Optional[datetime]
    last_updated_timestamp: Optional[datetime]

    def __init__(
        self,
        *,
        name: str,
        join_keys: Optional[Union[List[str], Dict[str, ValueType]]] = None,
        value_type: Optional[ValueType] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
    ):
        """
        Creates an Entity object.

        Args:
            name: The unique name of the entity.
            join_keys (optional): Either a list of join key names (legacy format) or a dictionary
                mapping join key names to their ValueTypes (new format). When using the list format,
                only a single join key is currently supported. When using the dictionary format,
                multiple join keys with different types are supported.
            value_type (optional): The type of the entity (used with legacy list format). If not
                specified, it will be inferred from the schema of the underlying data source.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the entity, typically the email of the primary maintainer.

        Raises:
            ValueError: Parameters are specified incorrectly.
        """
        self.name = name
        self.description = description
        self.tags = tags if tags is not None else {}
        self.owner = owner
        self.created_timestamp = None
        self.last_updated_timestamp = None

        # Handle join_keys - support both legacy List[str] and new Dict[str, ValueType] formats
        if join_keys:
            if isinstance(join_keys, list):
                # Legacy format: List[str]
                if len(join_keys) > 1:
                    raise ValueError(
                        "Multiple join keys not yet supported in list format. "
                        "Use Dict[str, ValueType] format instead: "
                        f"join_keys={{'{join_keys[0]}': ValueType.STRING, '{join_keys[1]}': ValueType.STRING}}"
                    )
                elif len(join_keys) == 1:
                    self.join_keys = {join_keys[0]: value_type or ValueType.UNKNOWN}
                else:
                    # Empty list - use entity name as default
                    self.join_keys = {name: value_type or ValueType.UNKNOWN}
            elif isinstance(join_keys, dict):
                # New format: Dict[str, ValueType]
                if not join_keys:
                    # Empty dict - use entity name as default
                    self.join_keys = {name: value_type or ValueType.UNKNOWN}
                else:
                    self.join_keys = join_keys.copy()
            else:
                raise ValueError(
                    "join_keys must be either List[str] (legacy) or Dict[str, ValueType] (new format)"
                )
        else:
            # No join_keys specified - use entity name as default
            self.join_keys = {name: value_type or ValueType.UNKNOWN}

        # Backward compatibility properties
        self.join_key = list(self.join_keys.keys())[0]
        self.value_type = list(self.join_keys.values())[0]

        # Warn about missing value_type for backward compatibility
        if value_type is None and len(self.join_keys) == 1:
            warnings.warn(
                "Entity value_type will be mandatory in the next release. "
                "Please specify a value_type for entity '%s'." % name,
                DeprecationWarning,
                stacklevel=2,
            )

    def __repr__(self):
        return (
            f"Entity(\n"
            f"    name={self.name!r},\n"
            f"    value_type={self.value_type!r},\n"
            f"    join_key={self.join_key!r},\n"
            f"    join_keys={self.join_keys!r},\n"
            f"    description={self.description!r},\n"
            f"    tags={self.tags!r},\n"
            f"    owner={self.owner!r},\n"
            f"    created_timestamp={self.created_timestamp!r},\n"
            f"    last_updated_timestamp={self.last_updated_timestamp!r}\n"
            f")"
        )

    def __hash__(self) -> int:
        return hash((self.name, tuple(sorted(self.join_keys.items()))))

    def __eq__(self, other):
        if not isinstance(other, Entity):
            raise TypeError("Comparisons should only involve Entity class objects.")

        if (
            self.name != other.name
            or self.join_keys != other.join_keys
            or self.description != other.description
            or self.tags != other.tags
            or self.owner != other.owner
        ):
            return False

        return True

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __lt__(self, other):
        return self.name < other.name

    def get_value_types(self) -> List[ValueType]:
        """
        Get all value types for the join keys.

        Returns:
            List of ValueType enums corresponding to each join key.
        """
        return list(self.join_keys.values())

    def get_join_key_names(self) -> List[str]:
        """
        Get all join key names.

        Returns:
            List of join key names.
        """
        return list(self.join_keys.keys())

    def get_join_key_value_type(self, join_key: str) -> ValueType:
        """
        Get the value type for a specific join key.

        Args:
            join_key: The name of the join key.

        Returns:
            The ValueType for the specified join key.

        Raises:
            KeyError: If the join key doesn't exist.
        """
        return self.join_keys[join_key]

    def is_valid(self):
        """
        Validates the state of this entity locally.

        Raises:
            ValueError: The entity does not have a name or does not have valid join keys.
        """
        if not self.name:
            raise ValueError("The entity does not have a name.")

        if not self.join_keys:
            raise ValueError(f"The entity {self.name} does not have any join keys.")

        for join_key, value_type in self.join_keys.items():
            if not join_key:
                raise ValueError(f"The entity {self.name} has an empty join key name.")
            if not value_type or value_type == ValueType.UNKNOWN:
                raise ValueError(
                    f"The entity {self.name} join key '{join_key}' has an invalid type."
                )

    @classmethod
    def from_proto(cls, entity_proto: EntityProto):
        """
        Creates an entity from a protobuf representation of an entity.

        Args:
            entity_proto: A protobuf representation of an entity.

        Returns:
            An Entity object based on the entity protobuf.
        """
        spec = entity_proto.spec

        # Check if the new join_keys format is available
        if spec.join_keys and len(spec.join_keys) > 0:
            # New format: use join_keys map
            join_keys_dict = {}
            for join_key_name, join_key_spec in spec.join_keys.items():
                join_keys_dict[join_key_name] = ValueType(join_key_spec.value_type)

            entity = cls(
                name=spec.name,
                join_keys=join_keys_dict,
                description=spec.description,
                tags=dict(spec.tags),
                owner=spec.owner,
            )
        else:
            # Legacy format: use single join_key and value_type
            entity = cls(
                name=spec.name,
                join_keys=[spec.join_key],
                value_type=ValueType(spec.value_type),
                description=spec.description,
                tags=dict(spec.tags),
                owner=spec.owner,
            )

        if entity_proto.meta.HasField("created_timestamp"):
            entity.created_timestamp = entity_proto.meta.created_timestamp.ToDatetime()
        if entity_proto.meta.HasField("last_updated_timestamp"):
            entity.last_updated_timestamp = (
                entity_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return entity

    def to_proto(self) -> EntityProto:
        """
        Converts an entity object to its protobuf representation.

        Returns:
            An EntityProto protobuf.
        """
        meta = EntityMetaProto()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)

        # Create join key specs for the new format
        join_keys_spec = {}
        for join_key_name, value_type in self.join_keys.items():
            from feast.protos.feast.core.Entity_pb2 import JoinKeySpec

            join_keys_spec[join_key_name] = JoinKeySpec(
                name=join_key_name,
                value_type=value_type.value,
                description="",  # Could be extended to support per-key descriptions
            )

        spec = EntitySpecProto(
            name=self.name,
            # Backward compatibility fields
            value_type=self.value_type.value,
            join_key=self.join_key,
            # New fields
            join_keys=join_keys_spec,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        return EntityProto(spec=spec, meta=meta)
