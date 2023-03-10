package cessfc

import (
	"encoding/binary"
	"sync"
	"time"
)

const (
	// Tcp message interval
	TCP_Message_Interval = time.Duration(time.Millisecond * 10)
	// Number of tcp message caches
	TCP_Message_Send_Buffers = 10
	TCP_Message_Read_Buffers = 10
	//
	TCP_SendBuffer = 8192
	TCP_ReadBuffer = 12000
	//
	Tcp_Dial_Timeout = time.Duration(time.Second * 5)
)

type FileStoreInfo struct {
	FileId      string         `json:"file_id"`
	FileState   string         `json:"file_state"`
	FileSize    int64          `json:"file_size"`
	IsUpload    bool           `json:"is_upload"`
	IsCheck     bool           `json:"is_check"`
	IsShard     bool           `json:"is_shard"`
	IsScheduler bool           `json:"is_scheduler"`
	Miners      map[int]string `json:"miners"`
}

type FileStoreInfoReceiver interface {
	Receive(fsi *FileStoreInfo)
}

type MsgType byte

const (
	MsgInvalid MsgType = iota
	MsgHead
	MsgFile
	MsgEnd
	MsgNotify
	MsgClose
	MsgRecvHead
	MsgRecvFile
	MsgFileSt
)

const (
	FileType_file   uint8 = 1
	FileType_filler uint8 = 2
)

type Status byte

const (
	Status_Ok Status = iota
	Status_Err
)

type Message struct {
	Pubkey   []byte  `json:"pubkey"`
	SignMsg  []byte  `json:"signmsg"`
	Sign     []byte  `json:"sign"`
	Bytes    []byte  `json:"bytes"`
	FileName string  `json:"filename"`
	FileHash string  `json:"filehash"`
	FileSize uint64  `json:"filesize"`
	MsgType  MsgType `json:"msgtype"`
	LastMark bool    `json:"lastmark"`
	FileType uint8   `json:"filetype"`
}

type Notify struct {
	Status byte
}

var (
	sendBufPool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, TCP_SendBuffer)
		},
	}

	readBufPool = &sync.Pool{
		New: func() any {
			return make([]byte, TCP_ReadBuffer)
		},
	}
)

func NewNotifyMsg(fileName string, status Status) *Message {
	m := &Message{}
	m.MsgType = MsgNotify
	m.Bytes = []byte{byte(status)}
	m.FileName = fileName
	m.FileHash = ""
	m.FileSize = 0
	m.LastMark = false
	m.FileType = FileType_file
	m.Pubkey = nil
	m.SignMsg = nil
	m.Sign = nil
	return m
}

func NewHeadMsg(fileName string, fid string, lastmark bool, pkey, signmsg, sign []byte) *Message {
	m := &Message{}
	m.MsgType = MsgHead
	m.FileName = fileName
	m.FileHash = fid
	m.FileSize = 0
	m.LastMark = lastmark
	m.FileType = FileType_file
	m.Pubkey = pkey
	m.SignMsg = signmsg
	m.Sign = sign
	m.Bytes = nil
	return m
}

func NewRecvHeadMsg(fid string, pkey, signmsg, sign []byte) *Message {
	m := &Message{}
	m.MsgType = MsgRecvHead
	m.FileName = fid
	m.FileHash = fid
	m.FileSize = 0
	m.LastMark = false
	m.FileType = FileType_file
	m.Pubkey = pkey
	m.SignMsg = signmsg
	m.Sign = sign
	m.Bytes = nil
	return m
}

func NewRecvFileMsg(fid string) *Message {
	m := &Message{}
	m.MsgType = MsgRecvFile
	m.FileName = fid
	m.FileHash = ""
	m.FileSize = 0
	m.LastMark = false
	m.FileType = FileType_file
	m.Pubkey = nil
	m.SignMsg = nil
	m.Sign = nil
	m.Bytes = nil
	return m
}

func NewFileMsg(fileName string, buflen int, buf []byte) *Message {
	m := &Message{}
	m.MsgType = MsgFile
	m.FileType = FileType_file
	m.FileName = fileName
	m.FileHash = ""
	m.FileSize = uint64(buflen)
	m.LastMark = false
	m.Pubkey = nil
	m.SignMsg = nil
	m.Sign = nil
	m.Bytes = sendBufPool.Get().([]byte)
	copy(m.Bytes, buf)
	return m
}

func NewEndMsg(fileName, fileHash string, size, originSize uint64, lastmark bool) *Message {
	m := &Message{}
	uintbytes := make([]byte, 8)
	binary.BigEndian.PutUint64(uintbytes, originSize)
	m.SignMsg = uintbytes
	m.MsgType = MsgEnd
	m.FileName = fileName
	m.FileHash = fileHash
	m.FileSize = size
	m.FileType = FileType_file
	m.LastMark = lastmark
	m.Pubkey = nil
	m.Sign = nil
	m.Bytes = nil
	return m
}

func NewCloseMsg(fileName string, status Status) *Message {
	m := &Message{}
	m.MsgType = MsgClose
	m.Bytes = []byte{byte(status)}
	m.FileName = fileName
	m.FileHash = ""
	m.FileSize = 0
	m.FileType = FileType_file
	m.LastMark = false
	m.Pubkey = nil
	m.SignMsg = nil
	m.Sign = nil
	return m
}

func NewFileStMsg(fid string) *Message {
	m := &Message{}
	m.MsgType = MsgFileSt
	m.FileName = ""
	m.FileHash = fid
	m.FileSize = 0
	m.LastMark = false
	m.FileType = FileType_file
	m.Pubkey = nil
	m.SignMsg = nil
	m.Sign = nil
	m.Bytes = nil
	return m
}
