package erasure

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	targetDir := "testdata/.test-tmp/encode"
	{
		cmd := exec.Command("rm", "-rf", targetDir)
		assert.NoError(t, cmd.Run())
	}
	{
		cmd := exec.Command("mkdir", "-p", targetDir)
		assert.NoError(t, cmd.Run())
	}

	shardCreater := func(index int) (io.WriteCloser, error) {
		return os.Create(filepath.Join(targetDir, fmt.Sprintf("testvideo.mp4.%d", index)))
	}
	shardOpener := func(index int) (io.ReadCloser, int64, error) {
		f, err := os.Open(filepath.Join(targetDir, fmt.Sprintf("testvideo.mp4.%d", index)))
		if err != nil {
			return nil, 0, err
		}
		fstat, err := f.Stat()
		if err != nil {
			return nil, 0, err
		}
		return f, fstat.Size(), nil
	}
	f, err := os.Open("testdata/testvideo.mp4")
	assert.NoError(t, err)
	rsece, err := NewRsecEncoderByFile(f, shardCreater, shardOpener)
	err = rsece.Encode()
	log.Println(rsece.DataShards(), rsece.ParityShards())
	assert.NoError(t, err)

	dirEntries, err := os.ReadDir(targetDir)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(dirEntries))
}

func splitFile(file *os.File, targetDir string) (*RsecEncoder, error) {
	filename := filepath.Base(file.Name())
	shardCreater := func(index int) (io.WriteCloser, error) {
		return os.Create(filepath.Join(targetDir, fmt.Sprintf("%s.%d", filename, index)))
	}
	shardOpener := func(index int) (io.ReadCloser, int64, error) {
		f, err := os.Open(filepath.Join(targetDir, fmt.Sprintf("%s.%d", filename, index)))
		if err != nil {
			return nil, 0, err
		}
		fstat, err := f.Stat()
		if err != nil {
			return nil, 0, err
		}
		return f, fstat.Size(), nil
	}
	rsece, err := NewRsecEncoderByFile(file, shardCreater, shardOpener)
	if err != nil {
		return nil, err
	}
	return rsece, rsece.Encode()
}

func TestDecode(t *testing.T) {
	srcDir := "testdata/.test-tmp/decode/"
	{
		cmd := exec.Command("rm", "-rf", srcDir)
		assert.NoError(t, cmd.Run())
	}
	shardsDir := filepath.Join(srcDir, "shards")
	{
		cmd := exec.Command("mkdir", "-p", shardsDir)
		assert.NoError(t, cmd.Run())
	}

	f, err := os.Open("testdata/testvideo.mp4")
	assert.NoError(t, err)
	_, err = splitFile(f, shardsDir)
	dirEntries, err := os.ReadDir(shardsDir)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(dirEntries))

	os.Remove(filepath.Join(shardsDir, dirEntries[4].Name()))
	os.Remove(filepath.Join(shardsDir, dirEntries[5].Name()))

	shardCreater := func(index int) (io.WriteCloser, error) {
		return os.Create(filepath.Join(shardsDir, fmt.Sprintf("testvideo.mp4.%d", index)))
	}
	shardOpener := func(index int) (io.ReadCloser, int64, error) {
		fp := filepath.Join(shardsDir, fmt.Sprintf("testvideo.mp4.%d", index))
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

	outputFilepath := filepath.Join(srcDir, "testvideo.mp4")
	output, err := os.Create(outputFilepath)
	assert.NoError(t, err)
	rsecd := NewRsecDecoder(output, 4, 2, shardOpener, shardCreater)
	err = rsecd.Decode()
	assert.NoError(t, err)

	fstat, err := os.Stat(outputFilepath)
	assert.NoError(t, err)
	assert.True(t, fstat.Size() > 0)

	srcFstat, err := os.Stat("testdata/testvideo.mp4")
	assert.NoError(t, err)
	assert.Equal(t, srcFstat.Size(), fstat.Size())
}
