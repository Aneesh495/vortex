// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Schema

import System
import System.Collections.Generic

// Compatibility modes governing schema evolution validation.
shared class CompatibilityMode {
    const None int16 = 0
    const Backward int16 = 1
    const Forward int16 = 2
    const Full int16 = 3
}

// Evaluates compatibility contracts across schema versions.
shared class CompatibilityValidator {
    // Validates whether newSchema is compatible with oldSchema under the given mode.
    func Validate(oldSchema SchemaDefinition, newSchema SchemaDefinition, mode int16) (bool, string) {
        if mode == CompatibilityMode.None {
            return (true, "")
        }

        // 1. Verify field type stability
        for i in 0 ... newSchema.Fields.Count {
            var newField = newSchema.Fields[i]
            var oldField = oldSchema.FindField(newField.Name)
            if oldField != nil {
                if oldField.TypeCode != newField.TypeCode {
                    return (false, "Field type changed for field: " + newField.Name)
                }
            }
        }

        // 2. Backward compatibility: new schema must be able to read data written by old schema.
        // Newly added fields in newSchema MUST be optional or have a default value.
        if mode == CompatibilityMode.Backward || mode == CompatibilityMode.Full {
            for i in 0 ... newSchema.Fields.Count {
                var newField = newSchema.Fields[i]
                var oldField = oldSchema.FindField(newField.Name)
                if oldField == nil {
                    if !newField.IsOptional && (newField.DefaultValue == nil || newField.DefaultValue.Length == 0) {
                        return (false, "New required field without default value breaks backward compatibility: " + newField.Name)
                    }
                }
            }
        }

        // 3. Forward compatibility: old schema must be able to read data written by new schema.
        // Fields removed from oldSchema MUST have been optional or had a default value.
        if mode == CompatibilityMode.Forward || mode == CompatibilityMode.Full {
            for i in 0 ... oldSchema.Fields.Count {
                var oldField = oldSchema.Fields[i]
                var newField = newSchema.FindField(oldField.Name)
                if newField == nil {
                    if !oldField.IsOptional && (oldField.DefaultValue == nil || oldField.DefaultValue.Length == 0) {
                        return (false, "Removed field without default breaks forward compatibility: " + oldField.Name)
                    }
                }
            }
        }

        return (true, "")
    }
}

// Represents a subject containing versioned schema evolutions.
class SchemaSubject {
    var Name string
    var Mode int16 = CompatibilityMode.Backward
    var Versions List[SchemaDefinition] = List[SchemaDefinition]()

    func Latest() SchemaDefinition? {
        if Versions.Count == 0 {
            return nil
        }
        return Versions[Versions.Count - 1]
    }

    func GetVersion(version int32) SchemaDefinition? {
        for i in 0 ... Versions.Count {
            var s = Versions[i]
            if s.Version == version {
                return s
            }
        }
        return nil
    }
}

// In-memory Schema Registry managing subject evolution, IDs, and compatibility checks.
class SchemaRegistry {
    var Subjects Dictionary[string, SchemaSubject] = Dictionary[string, SchemaSubject]()
    var SchemasById Dictionary[int32, SchemaDefinition] = Dictionary[int32, SchemaDefinition]()
    var NextSchemaId int32 = 1

    func GetSubject(subjectName string) SchemaSubject? {
        var sub SchemaSubject? = nil
        var found bool = Subjects.TryGetValue(subjectName, out sub)
        if found {
            return sub
        }
        return nil
    }

    func GetSchemaById(schemaId int32) SchemaDefinition? {
        var schema SchemaDefinition? = nil
        var found bool = SchemasById.TryGetValue(schemaId, out schema)
        if found {
            return schema
        }
        return nil
    }

    func SetCompatibility(subjectName string, mode int16) {
        var sub = GetSubject(subjectName)
        if sub != nil {
            sub.Mode = mode
        } else {
            var newSub = SchemaSubject()
            newSub.Name = subjectName
            newSub.Mode = mode
            Subjects[subjectName] = newSub
        }
    }

    func RegisterSchema(subjectName string, schema SchemaDefinition) (int32, int16, string) {
        var sub = GetSubject(subjectName)
        if sub == nil {
            sub = SchemaSubject()
            sub.Name = subjectName
            sub.Mode = CompatibilityMode.Backward
            Subjects[subjectName] = sub
        }

        var latest = sub.Latest()
        if latest != nil {
            var (isCompatible, errorMsg) = CompatibilityValidator.Validate(latest, schema, sub.Mode)
            if !isCompatible {
                return (-1, 409, errorMsg)
            }
            schema.Version = latest.Version + 1
        } else {
            schema.Version = 1
        }

        schema.SchemaId = NextSchemaId
        NextSchemaId = NextSchemaId + 1

        sub.Versions.Add(schema)
        SchemasById[schema.SchemaId] = schema

        return (schema.SchemaId, 0, "")
    }
}
