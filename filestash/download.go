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

package filestash

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"cmps/pkg/cessfc"
	"cmps/pkg/chain"
	"cmps/pkg/erasure"
	"cmps/pkg/utils"

	"github.com/pkg/errors"
)

func (t *FileStash) downloadFile(fileHash string, fmeta *chain.FileMetaInfo) (*FileBriefInfo, error) {
	fileHashDir := filepath.Join(t.fileStashDir, fileHash)
	if err := os.Mkdir(fileHashDir, 0755); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, errors.Wrap(err, "make filehash dir error")
	}
	tmpChunkDir, err := os.MkdirTemp(t.chunksDir, "down-chunks-")
	defer os.RemoveAll(tmpChunkDir)
	if err != nil {
		return nil, errors.Wrap(err, "make download tmp dir error")
	}

	var start = time.Now()
	err = t.downloadChunks(fmeta, tmpChunkDir)
	log.Printf("download %s chunks const: %dms \n", fileHash, time.Since(start).Milliseconds())

	shardCreater := func(index int) (io.WriteCloser, error) {
		f, err := os.Create(filepath.Join(tmpChunkDir, fmt.Sprintf("%s.%03d", fileHash, index)))
		if err != nil {
			return nil, err
		}
		return f, nil
	}
	shardOpener := func(index int) (io.ReadCloser, int64, error) {
		f, err := os.Open(filepath.Join(tmpChunkDir, fmt.Sprintf("%s.%03d", fileHash, index)))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, 0, nil
			} else {
				return nil, 0, err
			}
		}
		fstat, err := f.Stat()
		if err != nil {
			return nil, 0, err
		}
		return f, fstat.Size(), nil
	}

	log.Println("begin restore...")
	output, err := os.Create(filepath.Join(fileHashDir, "data"))
	minCountToDownload, parityShards := fmeta.MinChunkCountToDownload()
	start = time.Now()
	rsecd := erasure.NewRsecDecoder(output, minCountToDownload, parityShards, shardOpener, shardCreater)
	err = rsecd.Decode()
	if err != nil {
		return nil, errors.Wrap(err, "ReedSolomon restore error")
	}
	log.Printf("restore %s cost: %dus \n", fileHash, time.Since(start).Microseconds())

	sfm := SimpleFileMeta{fileHash, fmeta.FirstFilename()}
	t.storeSimpleFileMeta(&sfm)

	return &FileBriefInfo{
		OriginName: sfm.OriginName,
		FileHash:   fileHash,
		FilePath:   output.Name(),
		Size:       rsecd.OutputSize(),
	}, nil
}

func (t *FileStash) downloadChunks(fmeta *chain.FileMetaInfo, tmpChunkDir string) error {
	var err error
	var wg sync.WaitGroup
	minCountToDownload, _ := fmeta.MinChunkCountToDownload()
	wg.Add(minCountToDownload)
	for i := 0; i < minCountToDownload; i++ {
		chunk := fmeta.BlockInfo[i]
		go func(wg *sync.WaitGroup, chunk *chain.BlockInfo) {
			defer wg.Done()
			err = t.doDownloadChunk(chunk, tmpChunkDir)
			if err != nil {
				log.Printf("chunk: %s download error: %v", chunk.BlockId.String(), err)
			}
			//TODO: retry use remain chunk
		}(&wg, &chunk)
	}
	wg.Wait()
	log.Printf("%d/%d chunk downloaded\n", minCountToDownload, len(fmeta.BlockInfo))
	return err
}

func (t *FileStash) doDownloadChunk(chunk *chain.BlockInfo, tmpChunkDir string) error {
	log.Println("begin download chunk:", chunk.BlockId.String())
	var start time.Time
	msg := utils.GetRandomcode(16)
	// sign message
	sign, err := t.keyring.Sign(t.keyring.SigningContext([]byte(msg)))
	if err != nil {
		return err
	}

	start = time.Now()
	tcpAddr, err := net.ResolveTCPAddr("tcp", chunk.MinerIp.String())
	if err != nil {
		return err
	}
	log.Printf("resolve address cost: %dus", time.Since(start).Microseconds())

	start = time.Now()
	conTcp, err := net.DialTCP("tcp", nil, tcpAddr)
	log.Printf("dial to address %s cost: %dus", tcpAddr, time.Since(start).Microseconds())
	if err != nil {
		return err
	}

	srv := cessfc.NewClient(cessfc.NewTcp(conTcp), tmpChunkDir, nil)
	pubKey := t.keyring.Public()
	start = time.Now()
	err = srv.RecvFile(chunk.BlockId.String(), int64(chunk.BlockSize), pubKey[:], []byte(msg), sign[:])
	log.Printf("end download chunk: %s, cost: %dms \n", chunk.BlockId.String(), time.Since(start).Milliseconds())
	return err
}
