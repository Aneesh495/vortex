// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Native;

using System;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

/// <summary>
/// High-throughput vectorized memory operations, zero-allocation span hashing,
/// and low-level byte comparisons.
/// </summary>
public static class FastMemory
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool SequenceEqual(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
    {
        return first.SequenceEqual(second);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Hash64(ReadOnlySpan<byte> data)
    {
        const ulong fnvPrime = 0x100000001b3;
        ulong hash = 0xcbf29ce484222325;

        int length = data.Length;
        ref byte src = ref MemoryMarshal.GetReference(data);
        int offset = 0;

        while (offset + 8 <= length)
        {
            ulong val = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref src, offset));
            hash = (hash ^ val) * fnvPrime;
            offset += 8;
        }

        while (offset < length)
        {
            byte b = Unsafe.Add(ref src, offset);
            hash = (hash ^ b) * fnvPrime;
            offset++;
        }

        return hash;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteVarint32(Span<byte> destination, uint value)
    {
        int bytesWritten = 0;
        while (value >= 0x80)
        {
            destination[bytesWritten++] = (byte)(value | 0x80);
            value >>= 7;
        }
        destination[bytesWritten++] = (byte)value;
        return bytesWritten;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteVarint64(Span<byte> destination, ulong value)
    {
        int bytesWritten = 0;
        while (value >= 0x80)
        {
            destination[bytesWritten++] = (byte)(value | 0x80);
            value >>= 7;
        }
        destination[bytesWritten++] = (byte)value;
        return bytesWritten;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryReadVarint32(ReadOnlySpan<byte> source, out uint value, out int bytesRead)
    {
        value = 0;
        bytesRead = 0;
        int shift = 0;

        for (int i = 0; i < source.Length; i++)
        {
            byte b = source[i];
            bytesRead++;
            value |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return true;
            }
            shift += 7;
            if (shift >= 35)
            {
                return false;
            }
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryReadVarint64(ReadOnlySpan<byte> source, out ulong value, out int bytesRead)
    {
        value = 0;
        bytesRead = 0;
        int shift = 0;

        for (int i = 0; i < source.Length; i++)
        {
            byte b = source[i];
            bytesRead++;
            value |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return true;
            }
            shift += 7;
            if (shift >= 70)
            {
                return false;
            }
        }

        return false;
    }
}
