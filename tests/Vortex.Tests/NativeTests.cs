// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Tests;

using System;
using System.Text;
using Vortex.Native;
using Xunit;

public class NativeTests
{
    [Fact]
    public void Crc32C_EmptySpan_ReturnsZero()
    {
        uint crc = Crc32C.Compute(ReadOnlySpan<byte>.Empty);
        Assert.Equal(0u, crc);
    }

    [Fact]
    public void Crc32C_StandardAscii_MatchesKnownCheck()
    {
        // Standard ASCII check sequence "123456789"
        byte[] bytes = Encoding.ASCII.GetBytes("123456789");
        uint crc = Crc32C.Compute(bytes);
        // Castagnoli CRC32C of "123456789" is 0xE3069283
        Assert.Equal(0xE3069283u, crc);
    }

    [Fact]
    public void Crc32C_ThirtyTwoZeros_ComputesDeterministically()
    {
        byte[] bytes = new byte[32];
        uint crc = Crc32C.Compute(bytes);
        Assert.Equal(0x8A9136AAu, crc);
    }

    [Fact]
    public void FastMemory_Hash64_DifferentiatesInputs()
    {
        byte[] a = Encoding.UTF8.GetBytes("topic-alpha-0");
        byte[] b = Encoding.UTF8.GetBytes("topic-alpha-1");

        ulong hashA = FastMemory.Hash64(a);
        ulong hashB = FastMemory.Hash64(b);

        Assert.NotEqual(0UL, hashA);
        Assert.NotEqual(hashA, hashB);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(1u)]
    [InlineData(127u)]
    [InlineData(128u)]
    [InlineData(255u)]
    [InlineData(16384u)]
    [InlineData(1048576u)]
    [InlineData(uint.MaxValue)]
    public void FastMemory_Varint32_RoundTripsAccurately(uint original)
    {
        Span<byte> buffer = stackalloc byte[5];
        int written = FastMemory.WriteVarint32(buffer, original);
        Assert.True(written >= 1 && written <= 5);

        bool success = FastMemory.TryReadVarint32(buffer.Slice(0, written), out uint read, out int bytesRead);
        Assert.True(success);
        Assert.Equal(written, bytesRead);
        Assert.Equal(original, read);
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(1UL)]
    [InlineData(127UL)]
    [InlineData(128UL)]
    [InlineData(255UL)]
    [InlineData(16384UL)]
    [InlineData(1099511627776UL)]
    [InlineData(ulong.MaxValue)]
    public void FastMemory_Varint64_RoundTripsAccurately(ulong original)
    {
        Span<byte> buffer = stackalloc byte[10];
        int written = FastMemory.WriteVarint64(buffer, original);
        Assert.True(written >= 1 && written <= 10);

        bool success = FastMemory.TryReadVarint64(buffer.Slice(0, written), out ulong read, out int bytesRead);
        Assert.True(success);
        Assert.Equal(written, bytesRead);
        Assert.Equal(original, read);
    }
}
