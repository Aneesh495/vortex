// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Server

import System
import System.IO
import System.Text
import System.Net
import System.Threading
import System.Threading.Tasks
import System.Collections.Generic

// Runtime metrics tracking throughput, storage, and cluster performance.
class BrokerMetrics {
    var MessagesInTotal int64 = 0
    var BytesInTotal int64 = 0
    var BytesOutTotal int64 = 0
    var ActiveConnections int32 = 0
    var ActivePartitions int32 = 0
    var UnderReplicatedPartitions int32 = 0

    func RecordMessageIn(byteCount int32) {
        Interlocked.Increment(ref MessagesInTotal)
        Interlocked.Add(ref BytesInTotal, int64(byteCount))
    }

    func RecordMessageOut(byteCount int32) {
        Interlocked.Add(ref BytesOutTotal, int64(byteCount))
    }

    // Formats collected metrics into standard Prometheus exposition format.
    func ExportPrometheus() string {
        var sb = StringBuilder()
        sb.AppendLine("# HELP vortex_messages_in_total Total stream messages ingested")
        sb.AppendLine("# TYPE vortex_messages_in_total counter")
        sb.AppendLine("vortex_messages_in_total " + MessagesInTotal.ToString())

        sb.AppendLine("# HELP vortex_bytes_in_total Total bytes ingested")
        sb.AppendLine("# TYPE vortex_bytes_in_total counter")
        sb.AppendLine("vortex_bytes_in_total " + BytesInTotal.ToString())

        sb.AppendLine("# HELP vortex_bytes_out_total Total bytes egressed to consumers")
        sb.AppendLine("# TYPE vortex_bytes_out_total counter")
        sb.AppendLine("vortex_bytes_out_total " + BytesOutTotal.ToString())

        sb.AppendLine("# HELP vortex_active_partitions Total partition logs managed")
        sb.AppendLine("# TYPE vortex_active_partitions gauge")
        sb.AppendLine("vortex_active_partitions " + ActivePartitions.ToString())

        sb.AppendLine("# HELP vortex_active_connections Current client TCP socket connections")
        sb.AppendLine("# TYPE vortex_active_connections gauge")
        sb.AppendLine("vortex_active_connections " + ActiveConnections.ToString())

        return sb.ToString()
    }
}

// Embedded lightweight HTTP server providing telemetry endpoints.
class EmbeddedHttpServer {
    var Port int32 = 9100
    var Broker VortexBroker
    var Metrics BrokerMetrics
    var Listener HttpListener? = nil
    var Cts CancellationTokenSource = CancellationTokenSource()
    var IsRunning bool = false

    func Start() {
        if !HttpListener.IsSupported {
            return
        }

        var l = HttpListener()
        l.Prefixes.Add("http://127.0.0.1:" + Port.ToString() + "/")
        l.Start()
        Listener = l
        IsRunning = true

        Task.Run(ListenLoop)
    }

    func Stop() {
        IsRunning = false
        Cts.Cancel()
        if let l = Listener {
            l.Stop()
            l.Close()
        }
    }

    func ListenLoop() {
        while IsRunning && !Cts.IsCancellationRequested {
            if let l = Listener {
                try {
                    var ctx = l.GetContext()
                    HandleRequest(ctx)
                } catch (ex Exception) {
                    if !IsRunning {
                        break
                    }
                }
            } else {
                break
            }
        }
    }

    func HandleRequest(ctx HttpListenerContext) {
        var req = ctx.Request
        var resp = ctx.Response
        var raw string? = req.RawUrl
        var path string = raw != nil ? raw : "/"

        if path == "/" || path == "/dashboard" {
            var htmlPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "dashboard/index.html")
            var html = ""
            if File.Exists(htmlPath) {
                html = File.ReadAllText(htmlPath)
            } else if File.Exists("src/Vortex.Server/dashboard/index.html") {
                html = File.ReadAllText("src/Vortex.Server/dashboard/index.html")
            } else {
                html = "<html><body><h1>Vortex Streaming Cluster</h1><p>Online</p></body></html>"
            }
            var data []byte = Encoding.UTF8.GetBytes(html)
            resp.ContentType = "text/html; charset=utf-8"
            resp.ContentLength64 = int64(data.Length)
            resp.OutputStream.Write(data, 0, data.Length)
            resp.Close()
        } else if path == "/metrics" {
            var text = Metrics.ExportPrometheus()
            var data []byte = Encoding.UTF8.GetBytes(text)
            resp.ContentType = "text/plain; version=0.0.4"
            resp.ContentLength64 = int64(data.Length)
            resp.OutputStream.Write(data, 0, data.Length)
            resp.Close()
        } else if path == "/api/health" {
            var json = "{\"status\":\"UP\",\"brokerId\":" + Broker.Config.BrokerId.ToString() + "}"
            var data []byte = Encoding.UTF8.GetBytes(json)
            resp.ContentType = "application/json"
            resp.ContentLength64 = int64(data.Length)
            resp.OutputStream.Write(data, 0, data.Length)
            resp.Close()
        } else if path == "/api/topics" {
            var sb = StringBuilder()
            sb.Append("{\"topics\":[")
            for i in 0 ... Broker.TopicList.Count {
                var t = Broker.TopicList[i]
                var count int32 = Broker.Topics[t]
                if i > 0 { sb.Append(",") }
                sb.Append("{\"name\":\"" + t + "\",\"partitions\":" + count.ToString() + "}")
            }
            sb.Append("]}")
            var data []byte = Encoding.UTF8.GetBytes(sb.ToString())
            resp.ContentType = "application/json"
            resp.ContentLength64 = int64(data.Length)
            resp.OutputStream.Write(data, 0, data.Length)
            resp.Close()
        } else {
            resp.StatusCode = 404
            var data []byte = Encoding.UTF8.GetBytes("Not Found")
            resp.ContentLength64 = int64(data.Length)
            resp.OutputStream.Write(data, 0, data.Length)
            resp.Close()
        }
    }
}
