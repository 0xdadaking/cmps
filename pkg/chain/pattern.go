/*
   Copyright 2022 CESS scheduler authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package chain

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"github.com/pkg/errors"
)

const (
	ERR_Failed  = "failed"
	ERR_Timeout = "timeout"
	ERR_Empty   = "empty"
)

// error type
var (
	ERR_RPC_CONNECTION  = errors.New("rpc connection failed")
	ERR_RPC_IP_FORMAT   = errors.New("unsupported ip format")
	ERR_RPC_TIMEOUT     = errors.New("timeout")
	ERR_RPC_EMPTY_VALUE = errors.New("empty")
)

const fileHashLen = 64
const fileBlockIdLen = 68

type FileHash [fileHashLen]types.U8
type FileBlockId [fileBlockIdLen]types.U8

func NewFileHash(str string) (*FileHash, error) {
	if len(str) != fileHashLen {
		return nil, fmt.Errorf("invaild FileHash as length")
	}
	bytes := []byte(str)
	t := [fileHashLen]types.U8{}
	for i, b := range bytes {
		t[i] = types.NewU8(b)
	}
	f := FileHash(t)
	return &f, nil
}

func NewFileBlockId(str string) (*FileBlockId, error) {
	if len(str) != fileBlockIdLen {
		return nil, fmt.Errorf("invaild FileBlockId as length")
	}
	bytes := []byte(str)
	t := [fileBlockIdLen]types.U8{}
	for i, b := range bytes {
		t[i] = types.NewU8(b)
	}
	f := FileBlockId(t)
	return &f, nil
}

func (t FileBlockId) String() string {
	return string(t[:])
}

// storage miner info
type MinerInfo struct {
	PeerId      types.U64
	IncomeAcc   types.AccountID
	Ip          Ipv4Type
	Collaterals types.U128
	State       types.Bytes
	Power       types.U128
	Space       types.U128
	RewardInfo  RewardInfo
}

type RewardInfo struct {
	Total       types.U128
	Received    types.U128
	NotReceived types.U128
}

// cache storage miner
type Cache_MinerInfo struct {
	Peerid uint64 `json:"peerid"`
	Ip     string `json:"ip"`
}

// file meta info
type FileMetaInfo struct {
	Size       types.U64
	Index      types.U32
	State      types.Bytes
	UserBriefs []UserBrief
	BlockInfo  []BlockInfo
}

func (t FileMetaInfo) String() string {
	return fmt.Sprintf("FileMetaInfo{Size:%d,Index:%d,State:%s,UserBriefs:%v,BlockInfo[%d]:[%v]}",
		uint64(t.Size),
		uint32(t.Index),
		string(t.State),
		t.UserBriefs,
		len(t.BlockInfo),
		t.BlockInfo,
	)
}

func (t FileMetaInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Size       uint64      `json:"size"`
		Index      uint32      `json:"index"`
		State      string      `json:"state"`
		UserBriefs []UserBrief `json:"userBriefs,omitempty"`
		BlockInfo  []BlockInfo `json:"blocks,omitempty"`
	}{
		uint64(t.Size),
		uint32(t.Index),
		string(t.State),
		t.UserBriefs,
		t.BlockInfo,
	})
}

func (t FileMetaInfo) MinChunkCountToDownload() (int, int) {
	chunkLen := len(t.BlockInfo)
	// the 3 number is the Erasure Coding copy count
	parityShards := chunkLen / 3
	return chunkLen - parityShards, parityShards
}

func (t *FileMetaInfo) FirstFilename() string {
	if t.UserBriefs == nil || len(t.UserBriefs) == 0 {
		return ""
	}
	return string(t.UserBriefs[0].File_name)
}

// file block info
type BlockInfo struct {
	MinerId   types.U64
	BlockSize types.U64
	BlockNum  types.U32
	BlockId   FileBlockId
	MinerIp   Ipv4Type
	MinerAcc  types.AccountID
}

func (t BlockInfo) String() string {
	return fmt.Sprintf("BlockInfo{MinerId:%d,BlockSize:%d,BlockNum:%d,BlockId:%s,MinerIp:%s,MinerAcc:%s}",
		t.MinerId,
		t.BlockSize,
		t.BlockNum,
		t.BlockId.String(),
		t.MinerIp,
		types.HexEncodeToString(t.MinerAcc[:]),
	)
}

func (t BlockInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Mid  uint64 `json:"minerId"`
		Size uint64 `json:"blockSize"`
		Num  uint32 `json:"blockNum"`
		Id   string `json:"blockId"`
		Ip   string `json:"minerIp"`
		Macc string `json:"minerAcc"`
	}{
		uint64(t.MinerId),
		uint64(t.BlockSize),
		uint32(t.BlockNum),
		t.BlockId.String(),
		t.MinerIp.String(),
		types.HexEncodeToString(t.MinerAcc[:]),
	})
}

// filler meta info
type FillerMetaInfo struct {
	Size      types.U64
	Index     types.U32
	BlockNum  types.U32
	BlockSize types.U32
	ScanSize  types.U32
	Acc       types.AccountID
	Hash      FileHash
}

// scheduler info
type SchedulerInfo struct {
	Ip             Ipv4Type
	StashUser      types.AccountID
	ControllerUser types.AccountID
}

type Ipv4Type_Query struct {
	Placeholder types.U8 //
	Index       types.U8
	Value       [4]types.U8
	Port        types.U16
}

type IpAddress struct {
	IPv4 Ipv4Type
	IPv6 Ipv6Type
}
type Ipv4Type struct {
	Index types.U8
	Value [4]types.U8
	Port  types.U16
}

func (t Ipv4Type) String() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d",
		t.Value[0],
		t.Value[1],
		t.Value[2],
		t.Value[3],
		t.Port,
	)
}

func NewIpv4Address(ip string, port uint16) (*Ipv4Type, error) {
	var ipBytes [4]types.U8
	var fields = strings.Split(ip, ".")
	if len(fields) == 0 && len(fields) >= 4 {
		return nil, fmt.Errorf("invalid ip address format")
	}
	for i, f := range fields {
		n, err := strconv.Atoi(f)
		if err != nil {
			return nil, err
		}
		ipBytes[i] = types.NewU8(uint8(n))
	}
	return &Ipv4Type{
		Index: types.NewU8(0),
		Value: ipBytes,
		Port:  types.NewU16(port),
	}, nil
}

type Ipv6Type struct {
	Index types.U8
	Value [8]types.U16
	Port  types.U16
}

// user space package Info
type SpacePackage struct {
	Space           types.U128
	Used_space      types.U128
	Remaining_space types.U128
	Tenancy         types.U32
	Package_type    types.U8
	Start           types.U32
	Deadline        types.U32
	State           types.Bytes
}

type BucketInfo struct {
	Total_capacity     types.U32
	Available_capacity types.U32
	Objects_num        types.U32
	Objects_list       []FileHash
	Authority          []types.AccountID
}

type UserBrief struct {
	User        types.AccountID
	File_name   types.Bytes
	Bucket_name types.Bytes
}

func (t UserBrief) String() string {
	return fmt.Sprintf("UserBrief{User:%s,File_name:%s,Bucket_name:%s}",
		types.HexEncodeToString(t.User[:]),
		string(t.File_name),
		string(t.Bucket_name),
	)
}

func (t UserBrief) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		User       string `json:"user"`
		FileName   string `json:"fileName"`
		BucketName string `json:"bucketName"`
	}{
		types.HexEncodeToString(t.User[:]),
		string(t.File_name),
		string(t.Bucket_name),
	})
}
