package erasure

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	shardCreater := func(index int) (io.WriteCloser, error) {
		return os.Create(filepath.Join("../../testdata/", fmt.Sprintf("testvideo.mp4.%d", index)))
	}
	shardOpener := func(index int) (io.ReadCloser, int64, error) {
		f, err := os.Open(filepath.Join("../../testdata/", fmt.Sprintf("testvideo.mp4.%d", index)))
		if err != nil {
			return nil, 0, err
		}
		fstat, err := f.Stat()
		if err != nil {
			return nil, 0, err
		}
		return f, fstat.Size(), nil
	}
	f, err := os.Open("../../testdata/testvideo.mp4")
	assert.NoError(t, err)
	rsece, err := NewRsecEncoderByFile(f, shardCreater, shardOpener)
	err = rsece.Encode()
	log.Println(rsece.DataShards(), rsece.ParityShards())
	assert.NoError(t, err)
}

func TestDecode(t *testing.T) {
	// {
	// 	cmd := exec.Command("mkdir", "-p", "../../testdata/decode/shards")
	// 	assert.NoError(t, cmd.Run())
	// }
	// {
	// 	cmd := exec.Command("cp", "../../testdata/testvideo.mp4.[0-3]", "../../testdata/decode/shards")
	// 	cmd.Stdout = os.Stdout
	// 	assert.NoError(t, cmd.Run())
	// }

	shardCreater := func(index int) (io.WriteCloser, error) {
		return os.Create(filepath.Join("../../testdata/decode/shards", fmt.Sprintf("testvideo.mp4.%d", index)))
	}
	shardOpener := func(index int) (io.ReadCloser, int64, error) {
		fp := filepath.Join("../../testdata/decode/shards", fmt.Sprintf("testvideo.mp4.%d", index))
		fstat, err := os.Stat(fp)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, 0, nil
			} else {
				return nil, 0, err
			}
		}
		f, err := os.Open(fp)
		if err != nil {
			return nil, 0, err
		}
		return f, fstat.Size(), nil
	}

	f, err := os.Create("../../testdata/decode/testvideo.mp4")
	assert.NoError(t, err)
	rsecd := NewRsecDecoder(f, 4, 2, shardOpener, shardCreater)
	err = rsecd.Decode()
	assert.NoError(t, err)
}
