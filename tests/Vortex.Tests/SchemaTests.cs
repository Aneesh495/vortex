// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

using System.IO;
using System.Text;
using Vortex.Schema;
using Xunit;

namespace Vortex.Tests;

public class SchemaTests
{
    [Fact]
    public void SchemaCodec_RoundTripsAllFieldTypes()
    {
        var schema = new SchemaDefinition { SchemaId = 101, Name = "SensorReading", Version = 1 };
        schema.AddField("isActive", FieldType.TypeBoolean, false, "true");
        schema.AddField("sensorId", FieldType.TypeInt32, false, "0");
        schema.AddField("timestamp", FieldType.TypeInt64, false, "0");
        schema.AddField("location", FieldType.TypeString, false, "unknown");
        schema.AddField("payload", FieldType.TypeBytes, true, "");

        var record = new DynamicRecord { Schema = schema };
        record.Set("isActive", true);
        record.Set("sensorId", 42);
        record.Set("timestamp", 1729800000L);
        record.Set("location", "datacenter-us-east-1");
        record.Set("payload", Encoding.UTF8.GetBytes("raw-telemetry-bytes"));

        using var ms = new MemoryStream();
        SchemaCodec.Serialize(ms, record);

        ms.Position = 0;
        var decoded = SchemaCodec.Deserialize(ms, schema);

        Assert.Equal(schema.SchemaId, decoded.Schema.SchemaId);
        Assert.Equal(42, decoded.GetInt32("sensorId"));
        Assert.Equal(1729800000L, decoded.GetInt64("timestamp"));
        Assert.Equal("datacenter-us-east-1", decoded.GetString("location"));

        var decodedBytes = decoded.Get("payload") as byte[];
        Assert.NotNull(decodedBytes);
        Assert.Equal("raw-telemetry-bytes", Encoding.UTF8.GetString(decodedBytes));
    }

    [Fact]
    public void SchemaCodec_InvalidMagicByte_ThrowsInvalidDataException()
    {
        var schema = new SchemaDefinition { SchemaId = 1, Name = "Simple", Version = 1 };
        schema.AddField("val", FieldType.TypeInt32, false, "0");

        using var ms = new MemoryStream();
        ms.WriteByte(0xFF); // Invalid magic byte
        ms.Position = 0;

        Assert.Throws<InvalidDataException>(() => SchemaCodec.Deserialize(ms, schema));
    }

    [Fact]
    public void SchemaCodec_SchemaIdMismatch_ThrowsInvalidDataException()
    {
        var schema1 = new SchemaDefinition { SchemaId = 1, Name = "Order", Version = 1 };
        schema1.AddField("id", FieldType.TypeInt32, false, "0");

        var schema2 = new SchemaDefinition { SchemaId = 2, Name = "OrderV2", Version = 2 };
        schema2.AddField("id", FieldType.TypeInt32, false, "0");

        var rec = new DynamicRecord { Schema = schema1 };
        rec.Set("id", 123);

        using var ms = new MemoryStream();
        SchemaCodec.Serialize(ms, rec);

        ms.Position = 0;
        // Attempt to deserialize payload with schema2 (expects SchemaId 2, got 1)
        Assert.Throws<InvalidDataException>(() => SchemaCodec.Deserialize(ms, schema2));
    }

    [Fact]
    public void SchemaRegistry_RegisterFirstVersion_Succeeds()
    {
        var registry = new SchemaRegistry();
        var s1 = new SchemaDefinition { Name = "Customer" };
        s1.AddField("id", FieldType.TypeInt32, false, "0");
        s1.AddField("email", FieldType.TypeString, false, "");

        var (schemaId, errCode, errMsg) = registry.RegisterSchema("customer-subject", s1);
        Assert.Equal(0, errCode);
        Assert.Equal(1, schemaId);
        Assert.Equal(1, s1.Version);

        var retrieved = registry.GetSchemaById(1);
        Assert.NotNull(retrieved);
        Assert.Equal("Customer", retrieved.Name);
    }

    [Fact]
    public void SchemaRegistry_BackwardEvolution_AllowedWhenOptionalOrHasDefault()
    {
        var registry = new SchemaRegistry();
        var s1 = new SchemaDefinition { Name = "Order" };
        s1.AddField("orderId", FieldType.TypeInt32, false, "0");
        registry.RegisterSchema("orders", s1);

        var s2 = new SchemaDefinition { Name = "Order" };
        s2.AddField("orderId", FieldType.TypeInt32, false, "0");
        s2.AddField("couponCode", FieldType.TypeString, true, ""); // Optional field

        var (s2Id, s2Code, _) = registry.RegisterSchema("orders", s2);
        Assert.Equal(0, s2Code);
        Assert.Equal(2, s2Id);
        Assert.Equal(2, s2.Version);

        var s3 = new SchemaDefinition { Name = "Order" };
        s3.AddField("orderId", FieldType.TypeInt32, false, "0");
        s3.AddField("couponCode", FieldType.TypeString, true, "");
        s3.AddField("currency", FieldType.TypeString, false, "USD"); // Field with default

        var (s3Id, s3Code, _) = registry.RegisterSchema("orders", s3);
        Assert.Equal(0, s3Code);
        Assert.Equal(3, s3Id);
        Assert.Equal(3, s3.Version);
    }

    [Fact]
    public void SchemaRegistry_BackwardEvolution_RejectsRequiredFieldWithoutDefault()
    {
        var registry = new SchemaRegistry();
        var s1 = new SchemaDefinition { Name = "Payment" };
        s1.AddField("amount", FieldType.TypeInt64, false, "0");
        registry.RegisterSchema("payments", s1);

        var s2 = new SchemaDefinition { Name = "Payment" };
        s2.AddField("amount", FieldType.TypeInt64, false, "0");
        s2.AddField("routingNumber", FieldType.TypeString, false, ""); // Required without default

        var (s2Id, s2Code, errMsg) = registry.RegisterSchema("payments", s2);
        Assert.Equal(409, s2Code);
        Assert.Equal(-1, s2Id);
        Assert.Contains("breaks backward compatibility", errMsg);
    }

    [Fact]
    public void SchemaRegistry_TypeStability_RejectsChangingFieldTypes()
    {
        var registry = new SchemaRegistry();
        var s1 = new SchemaDefinition { Name = "Device" };
        s1.AddField("id", FieldType.TypeInt32, false, "0");
        registry.RegisterSchema("devices", s1);

        var s2 = new SchemaDefinition { Name = "Device" };
        s2.AddField("id", FieldType.TypeString, false, ""); // Type changed from Int32 to String

        var (s2Id, s2Code, errMsg) = registry.RegisterSchema("devices", s2);
        Assert.Equal(409, s2Code);
        Assert.Equal(-1, s2Id);
        Assert.Contains("Field type changed", errMsg);
    }

    [Fact]
    public void SchemaRegistry_FetchSubjectAndVersions()
    {
        var registry = new SchemaRegistry();
        var s1 = new SchemaDefinition { Name = "User" };
        s1.AddField("uid", FieldType.TypeInt32, false, "0");
        registry.RegisterSchema("users", s1);

        var s2 = new SchemaDefinition { Name = "User" };
        s2.AddField("uid", FieldType.TypeInt32, false, "0");
        s2.AddField("tier", FieldType.TypeString, true, "");
        registry.RegisterSchema("users", s2);

        var subject = registry.GetSubject("users");
        Assert.NotNull(subject);
        Assert.Equal(2, subject.Versions.Count);

        var latest = subject.Latest();
        Assert.NotNull(latest);
        Assert.Equal(2, latest.Version);

        var v1 = subject.GetVersion(1);
        Assert.NotNull(v1);
        Assert.Equal(1, v1.Version);
        Assert.Single(v1.Fields);
    }
}
