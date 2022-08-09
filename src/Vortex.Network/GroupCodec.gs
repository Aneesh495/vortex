// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Network

import System
import System.Collections.Generic
import System.IO
import Vortex.Core

data class GroupProtocolMetadata(Name string, Metadata []byte)
data class GroupMemberMetadata(MemberId string, Metadata []byte)

class JoinGroupRequest {
    var GroupId string
    var SessionTimeoutMs int32 = 10000
    var RebalanceTimeoutMs int32 = 30000
    var MemberId string = ""
    var ProtocolType string = "consumer"
    var GroupProtocols List[GroupProtocolMetadata] = List[GroupProtocolMetadata]()

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteString(ms, GroupId)
        BinaryCodecs.WriteInt32BE(ms, SessionTimeoutMs)
        BinaryCodecs.WriteInt32BE(ms, RebalanceTimeoutMs)
        BinaryCodecs.WriteString(ms, MemberId)
        BinaryCodecs.WriteString(ms, ProtocolType)

        BinaryCodecs.WriteInt32BE(ms, GroupProtocols.Count)
        for i in 0 ... GroupProtocols.Count {
            var gp = GroupProtocols[i]
            BinaryCodecs.WriteString(ms, gp.Name)
            BinaryCodecs.WriteInt32BE(ms, gp.Metadata.Length)
            if gp.Metadata.Length > 0 {
                ms.Write(gp.Metadata, 0, gp.Metadata.Length)
            }
        }
        return ms.ToArray()
    }
}

class JoinGroupResponse {
    var ErrorCode int16 = 0
    var GenerationId int32 = 0
    var GroupProtocol string = ""
    var LeaderId string = ""
    var MemberId string = ""
    var Members List[GroupMemberMetadata] = List[GroupMemberMetadata]()

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteInt16BE(ms, ErrorCode)
        BinaryCodecs.WriteInt32BE(ms, GenerationId)
        BinaryCodecs.WriteString(ms, GroupProtocol)
        BinaryCodecs.WriteString(ms, LeaderId)
        BinaryCodecs.WriteString(ms, MemberId)

        BinaryCodecs.WriteInt32BE(ms, Members.Count)
        for i in 0 ... Members.Count {
            var m = Members[i]
            BinaryCodecs.WriteString(ms, m.MemberId)
            BinaryCodecs.WriteInt32BE(ms, m.Metadata.Length)
            if m.Metadata.Length > 0 {
                ms.Write(m.Metadata, 0, m.Metadata.Length)
            }
        }
        return ms.ToArray()
    }
}

data class GroupAssignment(MemberId string, Assignment []byte)

class SyncGroupRequest {
    var GroupId string
    var GenerationId int32
    var MemberId string
    var GroupAssignments List[GroupAssignment] = List[GroupAssignment]()

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteString(ms, GroupId)
        BinaryCodecs.WriteInt32BE(ms, GenerationId)
        BinaryCodecs.WriteString(ms, MemberId)

        BinaryCodecs.WriteInt32BE(ms, GroupAssignments.Count)
        for i in 0 ... GroupAssignments.Count {
            var ga = GroupAssignments[i]
            BinaryCodecs.WriteString(ms, ga.MemberId)
            BinaryCodecs.WriteInt32BE(ms, ga.Assignment.Length)
            if ga.Assignment.Length > 0 {
                ms.Write(ga.Assignment, 0, ga.Assignment.Length)
            }
        }
        return ms.ToArray()
    }
}

class SyncGroupResponse {
    var ErrorCode int16 = 0
    var Assignment []byte = [0]byte

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteInt16BE(ms, ErrorCode)
        BinaryCodecs.WriteInt32BE(ms, Assignment.Length)
        if Assignment.Length > 0 {
            ms.Write(Assignment, 0, Assignment.Length)
        }
        return ms.ToArray()
    }
}

shared class GroupCodec {
    func DecodeJoinRequest(payload []byte) JoinGroupRequest {
        var ms = MemoryStream(payload)
        var groupId string = BinaryCodecs.ReadString(ms)
        var sessTimeout int32 = BinaryCodecs.ReadInt32BE(ms)
        var rebTimeout int32 = BinaryCodecs.ReadInt32BE(ms)
        var memberId string = BinaryCodecs.ReadString(ms)
        var protocolType string = BinaryCodecs.ReadString(ms)

        var count int32 = BinaryCodecs.ReadInt32BE(ms)
        var protos = List[GroupProtocolMetadata]()
        for i in 0 ... count {
            var name string = BinaryCodecs.ReadString(ms)
            var mLen int32 = BinaryCodecs.ReadInt32BE(ms)
            var meta []byte = [mLen]byte
            if mLen > 0 {
                ms.Read(meta, 0, mLen)
            }
            protos.Add(GroupProtocolMetadata(name, meta))
        }

        var req = JoinGroupRequest{}
        req.GroupId = groupId
        req.SessionTimeoutMs = sessTimeout
        req.RebalanceTimeoutMs = rebTimeout
        req.MemberId = memberId
        req.ProtocolType = protocolType
        req.GroupProtocols = protos
        return req
    }

    func DecodeJoinResponse(payload []byte) JoinGroupResponse {
        var ms = MemoryStream(payload)
        var err int16 = BinaryCodecs.ReadInt16BE(ms)
        var genId int32 = BinaryCodecs.ReadInt32BE(ms)
        var proto string = BinaryCodecs.ReadString(ms)
        var leaderId string = BinaryCodecs.ReadString(ms)
        var memberId string = BinaryCodecs.ReadString(ms)

        var count int32 = BinaryCodecs.ReadInt32BE(ms)
        var members = List[GroupMemberMetadata]()
        for i in 0 ... count {
            var mId string = BinaryCodecs.ReadString(ms)
            var mLen int32 = BinaryCodecs.ReadInt32BE(ms)
            var meta []byte = [mLen]byte
            if mLen > 0 {
                ms.Read(meta, 0, mLen)
            }
            members.Add(GroupMemberMetadata(mId, meta))
        }

        var resp = JoinGroupResponse{}
        resp.ErrorCode = err
        resp.GenerationId = genId
        resp.GroupProtocol = proto
        resp.LeaderId = leaderId
        resp.MemberId = memberId
        resp.Members = members
        return resp
    }

    func DecodeSyncRequest(payload []byte) SyncGroupRequest {
        var ms = MemoryStream(payload)
        var groupId string = BinaryCodecs.ReadString(ms)
        var genId int32 = BinaryCodecs.ReadInt32BE(ms)
        var memberId string = BinaryCodecs.ReadString(ms)

        var count int32 = BinaryCodecs.ReadInt32BE(ms)
        var assigns = List[GroupAssignment]()
        for i in 0 ... count {
            var mId string = BinaryCodecs.ReadString(ms)
            var aLen int32 = BinaryCodecs.ReadInt32BE(ms)
            var aBytes []byte = [aLen]byte
            if aLen > 0 {
                ms.Read(aBytes, 0, aLen)
            }
            assigns.Add(GroupAssignment(mId, aBytes))
        }

        var req = SyncGroupRequest{}
        req.GroupId = groupId
        req.GenerationId = genId
        req.MemberId = memberId
        req.GroupAssignments = assigns
        return req
    }

    func DecodeSyncResponse(payload []byte) SyncGroupResponse {
        var ms = MemoryStream(payload)
        var err int16 = BinaryCodecs.ReadInt16BE(ms)
        var aLen int32 = BinaryCodecs.ReadInt32BE(ms)
        var aBytes []byte = [aLen]byte
        if aLen > 0 {
            ms.Read(aBytes, 0, aLen)
        }

        var resp = SyncGroupResponse{}
        resp.ErrorCode = err
        resp.Assignment = aBytes
        return resp
    }
}
