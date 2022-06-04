// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Tests;

using System;
using System.IO;
using Vortex.Core;
using Xunit;

public class CodecTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(123456)]
    [InlineData(-123456)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void ZigZag32_RoundTripsAccurately(int value)
    {
        uint encoded = BinaryCodecs.ZigZagEncode32(value);
        int decoded = BinaryCodecs.ZigZagDecode32(encoded);
        Assert.Equal(value, decoded);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(9876543210123L)]
    [InlineData(-9876543210123L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void ZigZag64_RoundTripsAccurately(long value)
    {
        ulong encoded = BinaryCodecs.ZigZagEncode64(value);
        long decoded = BinaryCodecs.ZigZagDecode64(encoded);
        Assert.Equal(value, decoded);
    }

    [Fact]
    public void BigEndianPrimitives_RoundTripCorrectly()
    {
        using var ms = new MemoryStream();
        BinaryCodecs.WriteInt16BE(ms, (short)-1234);
        BinaryCodecs.WriteInt32BE(ms, 0x12345678);
        BinaryCodecs.WriteInt64BE(ms, 0x0123456789ABCDEF);

        ms.Position = 0;
        short s = BinaryCodecs.ReadInt16BE(ms);
        int i = BinaryCodecs.ReadInt32BE(ms);
        long l = BinaryCodecs.ReadInt64BE(ms);

        Assert.Equal((short)-1234, s);
        Assert.Equal(0x12345678, i);
        Assert.Equal(0x0123456789ABCDEF, l);
    }

    [Fact]
    public void Varints_RoundTripOverStream()
    {
        using var ms = new MemoryStream();
        BinaryCodecs.WriteVarint32(ms, 300u);
        BinaryCodecs.WriteVarint64(ms, 1000000000000UL);

        ms.Position = 0;
        uint v32 = BinaryCodecs.ReadVarint32(ms);
        ulong v64 = BinaryCodecs.ReadVarint64(ms);

        Assert.Equal(300u, v32);
        Assert.Equal(1000000000000UL, v64);
    }

    [Fact]
    public void StringCodec_RoundTripsUtf8()
    {
        using var ms = new MemoryStream();
        string message = "vortex-high-throughput-distributed-commit-log";
        BinaryCodecs.WriteString(ms, message);

        ms.Position = 0;
        string recovered = BinaryCodecs.ReadString(ms);
        Assert.Equal(message, recovered);
    }
}
