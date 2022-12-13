package filestash

import (
	"cmps/pkg/chain"
	"cmps/pkg/confile"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"github.com/stretchr/testify/assert"
)

const fileId string = "d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4"

func newFileBlockId(str string) chain.FileBlockId {
	r, err := chain.NewFileBlockId(str)
	if err != nil {
		panic(err)
	}
	return *r
}

func newIpv4Address(ipStr string, port uint16) chain.Ipv4Type {
	r, err := chain.NewIpv4Address(ipStr, port)
	if err != nil {
		panic(err)
	}
	return *r
}

func buildFileMeta() (fileMeta chain.FileMetaInfo) {
	b, _ := types.HexDecodeString("0x1ec940be673d3613e94c4d44e3f4621422c1a0778a53a34b2b45f3118f823c03")
	minerAcc := types.NewAccountID(b)

	chunks := [6]chain.BlockInfo{}
	chunks[0] = chain.BlockInfo{
		MinerId:   types.NewU64(8),
		BlockSize: types.NewU64(11224153),
		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.000"),
		MinerIp:   newIpv4Address("127.0.0.1", 17001),
		MinerAcc:  minerAcc,
		BlockNum:  16,
	}
	chunks[1] = chain.BlockInfo{
		MinerId:   types.NewU64(8),
		BlockSize: types.NewU64(11224153),
		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.001"),
		MinerIp:   newIpv4Address("127.0.0.1", 17001),
		MinerAcc:  minerAcc,
		BlockNum:  16,
	}
	chunks[2] = chain.BlockInfo{
		MinerId:   types.NewU64(8),
		BlockSize: types.NewU64(11224153),
		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.002"),
		MinerIp:   newIpv4Address("127.0.0.1", 17001),
		MinerAcc:  minerAcc,
		BlockNum:  16,
	}
	chunks[3] = chain.BlockInfo{
		MinerId:   types.NewU64(8),
		BlockSize: types.NewU64(11224153),
		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.003"),
		MinerIp:   newIpv4Address("127.0.0.1", 17001),
		MinerAcc:  minerAcc,
		BlockNum:  16,
	}
	chunks[4] = chain.BlockInfo{
		MinerId:   types.NewU64(8),
		BlockSize: types.NewU64(11224153),
		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.004"),
		MinerIp:   newIpv4Address("127.0.0.1", 17001),
		MinerAcc:  minerAcc,
		BlockNum:  16,
	}
	chunks[5] = chain.BlockInfo{
		MinerId:   types.NewU64(8),
		BlockSize: types.NewU64(11224153),
		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.005"),
		MinerIp:   newIpv4Address("127.0.0.1", 17001),
		MinerAcc:  minerAcc,
		BlockNum:  16,
	}

	fileMeta = chain.FileMetaInfo{
		Size:      types.NewU64(44896612),
		Index:     1,
		State:     []byte("active"),
		BlockInfo: chunks[:],
		UserBriefs: []chain.UserBrief{
			{
				User:        types.NewAccountID(types.MustHexDecodeString("0x882be63cfe247ac0e62d42c59037de5fecc6283f2d5c3ca4696c489c7fd94720")),
				File_name:   []byte("testvideo.mp4"),
				Bucket_name: []byte("videown"),
			},
		},
	}
	return fileMeta
}

func buildCessc(cfg confile.Confiler) chain.Chainer {
	client, err := chain.NewChainClient(cfg.GetRpcAddr(), cfg.GetCtrlPrk(), 5*time.Second)
	if err != nil {
		panic(err)
	}
	return client
}

func TestDownloadChunk(t *testing.T) {
	cfg := confile.NewConfigfile()
	cfg.Parse("../conf.toml")
	fileMeta := buildFileMeta()
	fsth := Must.NewFileStash(cfg.GetDataDir(), cfg, buildCessc(cfg))

	tmpChunkDir, err := os.MkdirTemp(fsth.chunksDir, "down-chunks-")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpChunkDir)
	err = fsth.doDownloadChunk(&fileMeta.BlockInfo[0], tmpChunkDir)
	assert.NoError(t, err)

	fstat, err := os.Stat(filepath.Join(tmpChunkDir, fileMeta.BlockInfo[0].BlockId.String()))
	assert.NoError(t, err)
	assert.Equal(t, int64(fileMeta.BlockInfo[0].BlockSize), fstat.Size())
}

func TestDownloadFile(t *testing.T) {
	cfg := confile.NewConfigfile()
	cfg.Parse("../conf.toml")
	fileMeta := buildFileMeta()
	fsth := Must.NewFileStash(cfg.GetDataDir(), cfg, buildCessc(cfg))

	fbi, err := fsth.downloadFile(fileId, &fileMeta)
	assert.NoError(t, err)
	assert.Equal(t, int64(fileMeta.Size), fbi.Size)

	os.RemoveAll(fsth.Dir())
}
