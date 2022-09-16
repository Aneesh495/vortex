// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Client

import System
import System.Collections.Generic
import Vortex.Core

// Detailed metadata description of a topic and its partition topologies.
class TopicDescription {
    var Name string
    var PartitionCount int32
    var ReplicationFactor int16
    var Configs Dictionary[string, string] = Dictionary[string, string]()
}

// Administrative client for managing topics, partitions, and cluster metadata.
class VortexAdmin {
    var Topics Dictionary[string, TopicDescription] = Dictionary[string, TopicDescription]()
    var TopicNames List[string] = List[string]()

    // Creates a new topic in the cluster.
    func CreateTopic(name string, partitionCount int32, replicationFactor int16) int16 {
        if name == nil || name.Length == 0 {
            return VortexError.UnknownTopicOrPartition
        }
        if partitionCount <= 0 {
            return VortexError.CorruptMessage
        }
        if Topics.ContainsKey(name) {
            return VortexError.TopicAlreadyExists
        }

        var desc = TopicDescription()
        desc.Name = name
        desc.PartitionCount = partitionCount
        desc.ReplicationFactor = replicationFactor

        Topics[name] = desc
        TopicNames.Add(name)
        return VortexError.None
    }

    // Lists all registered topic names.
    func ListTopics() List[string] {
        var copy = List[string]()
        for i in 0 ... TopicNames.Count {
            copy.Add(TopicNames[i])
        }
        return copy
    }

    // Retrieves description metadata for a given topic.
    func DescribeTopic(name string) TopicDescription? {
        var desc TopicDescription? = nil
        var found bool = Topics.TryGetValue(name, out desc)
        if found {
            return desc
        }
        return nil
    }

    // Deletes an existing topic from the cluster.
    func DeleteTopic(name string) int16 {
        if !Topics.ContainsKey(name) {
            return VortexError.UnknownTopicOrPartition
        }

        Topics.Remove(name)
        TopicNames.Remove(name)
        return VortexError.None
    }
}
