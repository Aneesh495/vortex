// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Tests;

using System;
using System.Text;
using Vortex.Core;
using Xunit;

public class CoreTests
{
    [Fact]
    public void ByteBuffer_ReadWrite_RoundTripsBytes()
    {
        var buf = new ByteBuffer();
        buf.Data = new byte[64];

        buf.WriteByte(0x12);
        buf.WriteByte(0x34);
        byte[] payload = Encoding.UTF8.GetBytes("vortex");
        buf.Write(payload, 0, payload.Length);

        Assert.Equal(8, buf.ReadableBytes());
        Assert.Equal(0x12, buf.ReadByte());
        Assert.Equal(0x34, buf.ReadByte());

        byte[] dest = new byte[6];
        int read = buf.Read(dest, 0, 6);
        Assert.Equal(6, read);
        Assert.Equal("vortex", Encoding.UTF8.GetString(dest));
    }

    [Fact]
    public void MemoryPool_RentsAndRecyclesBuffers()
    {
        var pool = new MemoryPool();
        pool.BlockSize = 256;
        pool.MaxBlocks = 4;

        var buf1 = pool.Rent();
        Assert.NotNull(buf1);
        Assert.Equal(256, buf1.Capacity());

        buf1.WriteByte(0xAA);
        pool.Return(buf1);

        Assert.Equal(1, pool.AvailableBlocks());

        var buf2 = pool.Rent();
        Assert.Same(buf1, buf2);
        Assert.Equal(0, buf2.ReadableBytes()); // Verifies Reset() was called
    }

    [Fact]
    public void RingBuffer_EnqueuesAndDequeuesInOrder()
    {
        var ring = RingBufferFactory.Create<string>(4);
        Assert.Equal(4, ring.Capacity());
        Assert.True(ring.IsEmpty());

        Assert.True(ring.TryEnqueue("msg-1"));
        Assert.True(ring.TryEnqueue("msg-2"));
        Assert.True(ring.TryEnqueue("msg-3"));
        Assert.True(ring.TryEnqueue("msg-4"));

        Assert.True(ring.IsFull());
        Assert.False(ring.TryEnqueue("overflow")); // Should be rejected

        Assert.True(ring.TryDequeue(out string? val1));
        Assert.Equal("msg-1", val1);

        Assert.True(ring.TryEnqueue("msg-5")); // Replaces the slot via wrap-around

        Assert.True(ring.TryDequeue(out string? val2));
        Assert.Equal("msg-2", val2);

        Assert.True(ring.TryDequeue(out string? val3));
        Assert.Equal("msg-3", val3);

        Assert.True(ring.TryDequeue(out string? val4));
        Assert.Equal("msg-4", val4);

        Assert.True(ring.TryDequeue(out string? val5));
        Assert.Equal("msg-5", val5);

        Assert.True(ring.IsEmpty());
        Assert.False(ring.TryDequeue(out string? empty));
        Assert.Null(empty);
    }

    [Fact]
    public void CompressionRegistry_GZipCodec_CompressesAndDecompresses()
    {
        var codec = CompressionRegistry.Resolve(CompressionType.GZip);
        Assert.Equal(1, codec.Type());

        string originalText = "VORTEX_STREAMING_CLUSTER_TEST_PAYLOAD_".PadRight(4096, 'X');
        byte[] original = Encoding.UTF8.GetBytes(originalText);

        byte[] compressed = codec.Compress(original);
        Assert.True(compressed.Length < original.Length);

        byte[] decompressed = codec.Decompress(compressed);
        Assert.Equal(original.Length, decompressed.Length);
        Assert.Equal(originalText, Encoding.UTF8.GetString(decompressed));
    }
}
