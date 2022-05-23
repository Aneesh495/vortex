// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Native;

using System;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;

/// <summary>
/// Provides hardware-accelerated CRC32C calculation using ARM64 and x86 SSE4.2 intrinsics,
/// falling back to an unrolled slicing-by-8 lookup table implementation.
/// </summary>
public static class Crc32C
{
    private const uint Poly = 0x82F63B78;
    private static readonly uint[][] Table;

    static Crc32C()
    {
        Table = new uint[8][];
        for (int i = 0; i < 8; i++)
        {
            Table[i] = new uint[256];
        }

        for (uint i = 0; i < 256; i++)
        {
            uint res = i;
            for (int t = 0; t < 8; t++)
            {
                res = (res & 1) == 1 ? Poly ^ (res >> 1) : (res >> 1);
            }
            Table[0][i] = res;
        }

        for (int i = 0; i < 256; i++)
        {
            for (int t = 1; t < 8; t++)
            {
                Table[t][i] = Table[0][Table[t - 1][i] & 0xFF] ^ (Table[t - 1][i] >> 8);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        return Compute(0xFFFFFFFF, data) ^ 0xFFFFFFFF;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Compute(uint seed, ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
        {
            return seed;
        }

        if (Crc32.Arm64.IsSupported)
        {
            return ComputeArm64(seed, data);
        }

        if (Sse42.IsSupported)
        {
            return ComputeX86(seed, data);
        }

        return ComputeSoftware(seed, data);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static uint ComputeArm64(uint crc, ReadOnlySpan<byte> data)
    {
        int length = data.Length;
        ref byte src = ref MemoryMarshal.GetReference(data);
        int offset = 0;

        while (offset + 8 <= length)
        {
            ulong val = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref src, offset));
            crc = Crc32.Arm64.ComputeCrc32C(crc, val);
            offset += 8;
        }

        if (offset + 4 <= length)
        {
            uint val = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref src, offset));
            crc = Crc32.ComputeCrc32C(crc, val);
            offset += 4;
        }

        if (offset + 2 <= length)
        {
            ushort val = Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref src, offset));
            crc = Crc32.ComputeCrc32C(crc, val);
            offset += 2;
        }

        if (offset < length)
        {
            byte val = Unsafe.Add(ref src, offset);
            crc = Crc32.ComputeCrc32C(crc, val);
        }

        return crc;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static uint ComputeX86(uint crc, ReadOnlySpan<byte> data)
    {
        int length = data.Length;
        ref byte src = ref MemoryMarshal.GetReference(data);
        int offset = 0;

        if (Sse42.X64.IsSupported)
        {
            ulong crc64 = crc;
            while (offset + 8 <= length)
            {
                ulong val = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref src, offset));
                crc64 = Sse42.X64.Crc32(crc64, val);
                offset += 8;
            }
            crc = (uint)crc64;
        }

        while (offset + 4 <= length)
        {
            uint val = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref src, offset));
            crc = Sse42.Crc32(crc, val);
            offset += 4;
        }

        while (offset < length)
        {
            byte val = Unsafe.Add(ref src, offset);
            crc = Sse42.Crc32(crc, val);
            offset++;
        }

        return crc;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static uint ComputeSoftware(uint crc, ReadOnlySpan<byte> data)
    {
        int length = data.Length;
        ref byte src = ref MemoryMarshal.GetReference(data);
        int offset = 0;

        while (offset + 8 <= length)
        {
            uint one = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref src, offset)) ^ crc;
            uint two = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref src, offset + 4));

            crc = Table[7][one & 0xFF]
                ^ Table[6][(one >> 8) & 0xFF]
                ^ Table[5][(one >> 16) & 0xFF]
                ^ Table[4][one >> 24]
                ^ Table[3][two & 0xFF]
                ^ Table[2][(two >> 8) & 0xFF]
                ^ Table[1][(two >> 16) & 0xFF]
                ^ Table[0][two >> 24];

            offset += 8;
        }

        while (offset < length)
        {
            byte b = Unsafe.Add(ref src, offset);
            crc = Table[0][(crc ^ b) & 0xFF] ^ (crc >> 8);
            offset++;
        }

        return crc;
    }
}
