// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Schema

import System
import System.IO
import System.Collections.Generic
import Vortex.Core

// Enumeration of supported primitive data types in the Vortex schema system.
shared class FieldType {
    const TypeBoolean int16 = 1
    const TypeInt32 int16 = 2
    const TypeInt64 int16 = 3
    const TypeDouble int16 = 4
    const TypeString int16 = 5
    const TypeBytes int16 = 6
}

// Represents a named field specification in a schema definition.
class SchemaField {
    var Name string
    var TypeCode int16
    var IsOptional bool = false
    var DefaultValue string = ""
}

// Complete structural schema definition defining record contracts.
class SchemaDefinition {
    var SchemaId int32
    var Name string
    var Version int32
    var Fields List[SchemaField] = List[SchemaField]()

    func AddField(name string, typeCode int16, isOptional bool, defaultValue string) {
        var f = SchemaField()
        f.Name = name
        f.TypeCode = typeCode
        f.IsOptional = isOptional
        f.DefaultValue = defaultValue
        Fields.Add(f)
    }

    func FindField(name string) SchemaField? {
        for i in 0 ... Fields.Count {
            var f = Fields[i]
            if f.Name == name {
                return f
            }
        }
        return nil
    }
}

// In-memory record adhering to a dynamic SchemaDefinition.
class DynamicRecord {
    var Schema SchemaDefinition
    var Values Dictionary[string, Object] = Dictionary[string, Object]()

    func Set(field string, value Object) {
        Values[field] = value
    }

    func Get(field string) Object? {
        var obj Object? = nil
        var found bool = Values.TryGetValue(field, out obj)
        if found {
            return obj
        }
        return nil
    }

    func GetString(field string) string {
        var v = Get(field)
        if v != nil {
            var s string? = Convert.ToString(v)
            if s != nil {
                return s
            }
        }
        return ""
    }

    func GetInt32(field string) int32 {
        var v = Get(field)
        if v != nil {
            return Convert.ToInt32(v)
        }
        return 0
    }

    func GetInt64(field string) int64 {
        var v = Get(field)
        if v != nil {
            return Convert.ToInt64(v)
        }
        return 0
    }
}

// Serializer and deserializer for dynamic schema records.
shared class SchemaCodec {
    const MagicByte byte = 0x00

    func Serialize(stream Stream, record DynamicRecord) {
        // 1. Magic byte prefix
        stream.WriteByte(MagicByte)

        // 2. Schema ID (4 bytes BE)
        BinaryCodecs.WriteInt32BE(stream, record.Schema.SchemaId)

        // 3. Field values in schema definition order
        for i in 0 ... record.Schema.Fields.Count {
            var field = record.Schema.Fields[i]
            var val = record.Get(field.Name)

            if field.TypeCode == FieldType.TypeBoolean {
                var b bool = false
                if val != nil {
                    b = Convert.ToBoolean(val)
                }
                stream.WriteByte(b ? byte(1) : byte(0))
            } else if field.TypeCode == FieldType.TypeInt32 {
                var iVal int32 = 0
                if val != nil {
                    iVal = Convert.ToInt32(val)
                }
                BinaryCodecs.WriteInt32BE(stream, iVal)
            } else if field.TypeCode == FieldType.TypeInt64 {
                var lVal int64 = 0
                if val != nil {
                    lVal = Convert.ToInt64(val)
                }
                BinaryCodecs.WriteInt64BE(stream, lVal)
            } else if field.TypeCode == FieldType.TypeString {
                var sVal string = ""
                if val != nil {
                    var s string? = Convert.ToString(val)
                    if s != nil {
                        sVal = s
                    }
                }
                BinaryCodecs.WriteString(stream, sVal)
            } else if field.TypeCode == FieldType.TypeBytes {
                if val != nil {
                    var bytes = val as []byte
                    if bytes != nil {
                        BinaryCodecs.WriteInt32BE(stream, bytes.Length)
                        stream.Write(bytes, 0, bytes.Length)
                    } else {
                        BinaryCodecs.WriteInt32BE(stream, 0)
                    }
                } else {
                    BinaryCodecs.WriteInt32BE(stream, 0)
                }
            }
        }
    }

    func Deserialize(stream Stream, schema SchemaDefinition) DynamicRecord {
        var magic = stream.ReadByte()
        if magic != int32(MagicByte) {
            throw InvalidDataException("Invalid magic byte in schema envelope")
        }

        var schemaId int32 = BinaryCodecs.ReadInt32BE(stream)
        if schemaId != schema.SchemaId {
            throw InvalidDataException("Schema ID mismatch in binary payload")
        }

        var rec = DynamicRecord()
        rec.Schema = schema

        for i in 0 ... schema.Fields.Count {
            var field = schema.Fields[i]
            if field.TypeCode == FieldType.TypeBoolean {
                var b byte = byte(stream.ReadByte())
                rec.Set(field.Name, b == byte(1))
            } else if field.TypeCode == FieldType.TypeInt32 {
                var val int32 = BinaryCodecs.ReadInt32BE(stream)
                rec.Set(field.Name, val)
            } else if field.TypeCode == FieldType.TypeInt64 {
                var val int64 = BinaryCodecs.ReadInt64BE(stream)
                rec.Set(field.Name, val)
            } else if field.TypeCode == FieldType.TypeString {
                var val string = BinaryCodecs.ReadString(stream)
                rec.Set(field.Name, val)
            } else if field.TypeCode == FieldType.TypeBytes {
                var len int32 = BinaryCodecs.ReadInt32BE(stream)
                var buf []byte = [len]byte
                if len > 0 {
                    var readTotal int32 = 0
                    while readTotal < len {
                        var r int32 = stream.Read(buf, readTotal, len - readTotal)
                        if r <= 0 {
                            throw EndOfStreamException("Unexpected EOF reading schema byte field")
                        }
                        readTotal = readTotal + r
                    }
                }
                rec.Set(field.Name, buf)
            }
        }

        return rec
    }
}
