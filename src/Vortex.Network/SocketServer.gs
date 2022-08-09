// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Network

import System
import System.Collections.Concurrent
import System.IO
import System.Net
import System.Net.Sockets
import System.Threading
import System.Threading.Tasks
import Vortex.Core

data class InboundMessage(ConnectionId int64, Frame WireFrame)
data class OutboundMessage(ConnectionId int64, Frame WireFrame)

// High performance asynchronous TCP server leveraging G# channels for non-blocking packet routing.
class SocketServer {
    var Host string = "127.0.0.1"
    var Port int32 = 9092
    var Listener TcpListener? = nil
    var IsRunning bool = false
    var NextConnectionId int64 = 1
    var Connections ConcurrentDictionary[int64, TcpClient] = ConcurrentDictionary[int64, TcpClient]()
    var InboundChannel chan[InboundMessage] = chan[InboundMessage](4096)
    var Cts CancellationTokenSource = CancellationTokenSource()

    func Start() {
        var ip = IPAddress.Parse(Host)
        var l = TcpListener(ip, Port)
        l.Start()
        Listener = l
        IsRunning = true

        go AcceptConnections()
    }

    async func AcceptConnections() {
        while IsRunning && !Cts.IsCancellationRequested {
            try {
                if let l = Listener {
                    var client = await l.AcceptTcpClientAsync(Cts.Token)

                    var connId int64 = Interlocked.Increment(ref NextConnectionId)
                    Connections[connId] = client

                    go HandleClientRead(connId, client)
                }
            } catch (ex Exception) {
                if !IsRunning {
                    break
                }
            }
        }
    }

    async func HandleClientRead(connId int64, client TcpClient) {
        var stream = client.GetStream()
        while IsRunning && client.Connected && !Cts.IsCancellationRequested {
            try {
                var frame = WireFrameCodec.Deserialize(stream)
                InboundChannel <- InboundMessage(connId, frame)
            } catch (ex Exception) {
                break
            }
        }

        CloseConnection(connId)
    }

    func SendResponse(connId int64, frame WireFrame) bool {
        var client TcpClient? = nil
        var found bool = Connections.TryGetValue(connId, out client)
        if found && client != nil && client.Connected {
            try {
                var stream = client.GetStream()
                frame.Serialize(stream)
                stream.Flush()
                return true
            } catch (ex Exception) {
                CloseConnection(connId)
                return false
            }
        }
        return false
    }

    func CloseConnection(connId int64) {
        var client TcpClient? = nil
        var removed bool = Connections.TryRemove(connId, out client)
        if removed && client != nil {
            try {
                client.Close()
                client.Dispose()
            } catch (ex Exception) { }
        }
    }

    func Stop() {
        IsRunning = false
        Cts.Cancel()
        if let l = Listener {
            l.Stop()
        }

        var enumerator = Connections.GetEnumerator()
        while enumerator.MoveNext() {
            var pair = enumerator.Current
            try {
                pair.Value.Close()
                pair.Value.Dispose()
            } catch (ex Exception) { }
        }
        Connections.Clear()
        InboundChannel.Close()
    }
}
