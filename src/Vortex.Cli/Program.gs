// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Cli

import System
import System.IO
import System.Text
import System.Collections.Generic
import Vortex.Core
import Vortex.Client
import Vortex.Server

func PrintUsage() {
    Console.WriteLine("Vortex Streaming Cluster CLI")
    Console.WriteLine("Usage: vortex <command> [args...]")
    Console.WriteLine("")
    Console.WriteLine("Available commands:")
    Console.WriteLine("  topic create <name> <partitions>   Create a new topic")
    Console.WriteLine("  topic list                         List all registered topics")
    Console.WriteLine("  topic describe <name>              Describe topic partitions and configuration")
    Console.WriteLine("  produce <topic> <message>          Produce a message to a topic")
    Console.WriteLine("  consume <topic>                    Consume messages from a topic")
    Console.WriteLine("  bench [iterations] [payloadBytes]  Run native high-throughput benchmark")
    Console.WriteLine("  help                               Display this help message")
}

func Main(args []string) int32 {
    if args.Length == 0 || args[0] == "help" || args[0] == "--help" {
        PrintUsage()
        return 0
    }

    var cmd string = args[0]

    if cmd == "topic" {
        if args.Length < 2 {
            Console.WriteLine("Error: missing topic subcommand (create, list, describe)")
            return 1
        }

        var subCmd string = args[1]
        var admin = VortexAdmin()

        if subCmd == "create" {
            if args.Length < 4 {
                Console.WriteLine("Error: usage: vortex topic create <name> <partitions>")
                return 1
            }
            var topicName string = args[2]
            var partitions int32 = Convert.ToInt32(args[3])
            var err int16 = admin.CreateTopic(topicName, partitions, 1)
            if err == 0 {
                Console.WriteLine("Successfully created topic: " + topicName + " (" + partitions.ToString() + " partitions)")
                return 0
            } else {
                Console.WriteLine("Failed to create topic: " + VortexError.Describe(err))
                return 1
            }
        } else if subCmd == "list" {
            var topics = admin.ListTopics()
            Console.WriteLine("Topics (" + topics.Count.ToString() + "):")
            for i in 0 ... topics.Count {
                Console.WriteLine("  - " + topics[i])
            }
            return 0
        } else if subCmd == "describe" {
            if args.Length < 3 {
                Console.WriteLine("Error: usage: vortex topic describe <name>")
                return 1
            }
            var topicName string = args[2]
            var desc = admin.DescribeTopic(topicName)
            if desc != nil {
                Console.WriteLine("Topic: " + desc.Name)
                Console.WriteLine("  Partitions: " + desc.PartitionCount.ToString())
                Console.WriteLine("  Replication Factor: " + desc.ReplicationFactor.ToString())
                return 0
            } else {
                Console.WriteLine("Error: Topic not found: " + topicName)
                return 1
            }
        }
    } else if cmd == "produce" {
        if args.Length < 3 {
            Console.WriteLine("Error: usage: vortex produce <topic> <message>")
            return 1
        }
        var topic string = args[1]
        var msg string = args[2]

        var producer = VortexProducer()
        var meta = producer.Send(topic, nil, Encoding.UTF8.GetBytes(msg))
        Console.WriteLine("Produced record to " + meta.Topic + " [partition " + meta.Partition.ToString() + ", offset " + meta.Offset.ToString() + "]")
        return 0
    } else if cmd == "consume" {
        if args.Length < 2 {
            Console.WriteLine("Error: usage: vortex consume <topic>")
            return 1
        }
        var topic string = args[1]
        var consumer = VortexConsumer()
        var tp = TopicPartition(topic, 0)
        var partitions = List[TopicPartition]()
        partitions.Add(tp)
        consumer.Assign(partitions)

        Console.WriteLine("Subscribed to " + topic + "-0. Polling...")
        var records = consumer.Poll(1000)
        Console.WriteLine("Fetched " + records.Count.ToString() + " records.")
        for i in 0 ... records.Count {
            var r = records[i]
            Console.WriteLine("[" + r.Offset.ToString() + "] " + r.ValueString())
        }
        return 0
    } else if cmd == "bench" {
        var iterations int32 = 100000
        var payloadSize int32 = 256
        if args.Length >= 2 {
            iterations = Convert.ToInt32(args[1])
        }
        if args.Length >= 3 {
            payloadSize = Convert.ToInt32(args[2])
        }
        return RunBenchmark(iterations, payloadSize)
    }

    Console.WriteLine("Unknown command: " + cmd)
    PrintUsage()
    return 1
}

func RunBenchmark(iterations int32, payloadSize int32) int32 {
    Console.WriteLine("==================================================")
    Console.WriteLine("  VORTEX HIGH-THROUGHPUT ENGINE BENCHMARK")
    Console.WriteLine("==================================================")
    Console.WriteLine("Iterations:   " + iterations.ToString())
    Console.WriteLine("Payload size: " + payloadSize.ToString() + " bytes")

    var payload []byte = [payloadSize]byte
    for i in 0 ... payloadSize {
        payload[i] = byte(i % 256)
    }

    var producer = VortexProducer()
    producer.RegisterTopicMetadata("bench-topic", 8)

    var sw = System.Diagnostics.Stopwatch.StartNew()

    for i in 0 ... iterations {
        producer.Send("bench-topic", nil, payload)
    }

    var drained = producer.DrainBatches()
    sw.Stop()

    var elapsedSec double = sw.Elapsed.TotalSeconds
    var opsPerSec double = double(iterations) / elapsedSec
    var mbProcessed double = (double(iterations) * double(payloadSize)) / (1024.0 * 1024.0)
    var mbPerSec double = mbProcessed / elapsedSec

    Console.WriteLine("Elapsed time: " + sw.ElapsedMilliseconds.ToString() + " ms")
    Console.WriteLine("Throughput:   " + String.Format("{0:N0}", opsPerSec) + " msgs/sec")
    Console.WriteLine("Data rate:    " + String.Format("{0:N2}", mbPerSec) + " MB/sec")
    Console.WriteLine("Batches:      " + drained.Count.ToString())
    Console.WriteLine("==================================================")
    return 0
}
