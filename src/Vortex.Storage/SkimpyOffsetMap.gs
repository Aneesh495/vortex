// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Storage

import System
import Vortex.Native

// Compact open-addressing hash table mapping 64-bit key hashes to latest record offsets.
// Used by the log cleaner compaction engine for memory-efficient key deduplication.
class SkimpyOffsetMap {
    var Hashes []uint64
    var Offsets []int64
    var Capacity int32
    var Mask int32
    var EntryCount int32 = 0
    var MaxLoad int32

    func Count() int32 {
        return EntryCount
    }

    // Inserts or updates the latest offset for the given key bytes.
    func Put(key []byte, offset int64) bool {
        if key.Length == 0 {
            return false
        }
        if EntryCount >= MaxLoad {
            return false
        }

        var hash uint64 = FastMemory.Hash64(key)
        if hash == 0 {
            hash = 1
        }

        var slot int32 = int32(hash) & Mask
        while Hashes[slot] != 0 && Hashes[slot] != hash {
            slot = (slot + 1) & Mask
        }

        if Hashes[slot] == 0 {
            EntryCount = EntryCount + 1
        }

        Hashes[slot] = hash
        Offsets[slot] = offset
        return true
    }

    // Retrieves the latest offset for the key, or -1 if the key does not exist.
    func Get(key []byte) int64 {
        if key.Length == 0 || EntryCount == 0 {
            return -1
        }

        var hash uint64 = FastMemory.Hash64(key)
        if hash == 0 {
            hash = 1
        }

        var slot int32 = int32(hash) & Mask
        var startSlot int32 = slot

        while Hashes[slot] != 0 {
            if Hashes[slot] == hash {
                return Offsets[slot]
            }
            slot = (slot + 1) & Mask
            if slot == startSlot {
                break
            }
        }

        return -1
    }

    func Contains(key []byte) bool {
        return Get(key) >= 0
    }

    func Clear() {
        for i in 0 ... Capacity {
            Hashes[i] = 0
            Offsets[i] = -1
        }
        EntryCount = 0
    }
}

// Factory helper for initializing a SkimpyOffsetMap.
shared class SkimpyOffsetMapFactory {
    func Create(capacityPow2 int32) SkimpyOffsetMap {
        var cap int32 = 16
        while cap < capacityPow2 {
            cap = cap << 1
        }

        var offsetMap = SkimpyOffsetMap{}
        offsetMap.Capacity = cap
        offsetMap.Mask = cap - 1
        offsetMap.Hashes = [cap]uint64
        offsetMap.Offsets = [cap]int64
        offsetMap.MaxLoad = int32(cap * 3 / 4) // 75% load factor
        offsetMap.EntryCount = 0

        for i in 0 ... cap {
            offsetMap.Offsets[i] = -1
        }

        return offsetMap
    }
}
