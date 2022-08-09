// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Network

import System
import System.Collections.Concurrent
import System.Net

// Metadata and telemetry tracked for an active client connection session.
class ConnectionContext {
    var ConnectionId int64
    var RemoteEndpoint string = ""
    var ConnectedTimestampMs int64
    var LastActiveTimestampMs int64
    var TotalRequestsReceived int64 = 0
    var TotalBytesSent int64 = 0
    var TotalBytesReceived int64 = 0
    var Tokens float64 = 1000.0 // Token bucket rate limiter capacity
    var MaxTokens float64 = 1000.0
    var RefillRatePerSecond float64 = 500.0

    // Evaluates token bucket rate limiter to prevent denial-of-service / socket exhaustion.
    func TryAcquire(cost float64, currentTimestampMs int64) bool {
        var elapsedSeconds float64 = float64(currentTimestampMs - LastActiveTimestampMs) / 1000.0
        if elapsedSeconds > 0.0 {
            Tokens = Math.Min(MaxTokens, Tokens + (elapsedSeconds * RefillRatePerSecond))
            LastActiveTimestampMs = currentTimestampMs
        }

        if Tokens >= cost {
            Tokens = Tokens - cost
            return true
        }

        return false
    }
}

// Manages all active client connections, enforcing idle timeouts and rate limiting policies.
class ConnectionManager {
    var IdleTimeoutMs int64 = 60000 // 60 seconds idle timeout
    var Sessions ConcurrentDictionary[int64, ConnectionContext] = ConcurrentDictionary[int64, ConnectionContext]()

    func Register(connId int64, remoteEndpoint string, currentTimeMs int64) ConnectionContext {
        var ctx = ConnectionContext{}
        ctx.ConnectionId = connId
        ctx.RemoteEndpoint = remoteEndpoint
        ctx.ConnectedTimestampMs = currentTimeMs
        ctx.LastActiveTimestampMs = currentTimeMs
        Sessions[connId] = ctx
        return ctx
    }

    func RecordActivity(connId int64, bytesReceived int32, currentTimeMs int64) bool {
        var ctx ConnectionContext? = nil
        var found bool = Sessions.TryGetValue(connId, out ctx)
        if found && ctx != nil {
            ctx.TotalRequestsReceived = ctx.TotalRequestsReceived + 1
            ctx.TotalBytesReceived = ctx.TotalBytesReceived + int64(bytesReceived)
            return ctx.TryAcquire(1.0, currentTimeMs)
        }
        return false
    }

    func Unregister(connId int64) {
        var ctx ConnectionContext? = nil
        Sessions.TryRemove(connId, out ctx)
    }

    func ActiveCount() int32 {
        return Sessions.Count
    }
}
