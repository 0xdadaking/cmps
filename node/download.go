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

package node

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"cmps/pkg/chain"
	"cmps/pkg/erasure"
	"cmps/pkg/utils"

	cesskeyring "github.com/CESSProject/go-keyring"
)

type FileDownloadInfo struct {
	Id           string
	Meta         *chain.FileMetaInfo
	Path         string
	ParityShards int
}

func (n Node) downloadFile(fid string, fmeta *chain.FileMetaInfo, storageDir string) (*FileDownloadInfo, error) {
	var err error
	var start time.Time

	start = time.Now()
	err = n.downloadChunks(fmeta, storageDir)
	log.Printf("download %s chunks const: %dms \n", fid, time.Since(start).Milliseconds())

	log.Println("begin restore...")
	minCountToDownload, parityShards := fmeta.MinChunkCountToDownload()
	start = time.Now()
	err = erasure.ReedSolomon_Restore(storageDir, fid, minCountToDownload, parityShards, uint64(fmeta.Size))
	log.Printf("restore %s cost: %dus \n", fid, time.Since(start).Microseconds())
	if err != nil {
		return nil, fmt.Errorf("ReedSolomon_Restore error: %v", err)
	}

	return &FileDownloadInfo{
		Id:           fid,
		Meta:         fmeta,
		Path:         storageDir,
		ParityShards: parityShards,
	}, nil
}

func figureChunkFilePath(fmeta *chain.FileMetaInfo, index int, dir string) string {
	chunk := fmeta.BlockInfo[index]
	chunkFilePath := filepath.Join(dir, chunk.BlockId.String())
	if len(fmeta.BlockInfo) == 1 {
		chunkFilePath = chunkFilePath[:(len(chunkFilePath) - 4)]
	}
	return chunkFilePath
}

func (n Node) downloadChunks(fmeta *chain.FileMetaInfo, storageDir string) error {
	var err error
	var wg sync.WaitGroup
	minCountToDownload, _ := fmeta.MinChunkCountToDownload()
	wg.Add(minCountToDownload)
	for i := 0; i < minCountToDownload; i++ {
		chunk := fmeta.BlockInfo[i]
		chunkFilePath := figureChunkFilePath(fmeta, i, storageDir)
		// if err := n.doDownloadChunk(chunkFilePath, &chunk); err != nil {
		// 	log.Printf("chunk: %s download error: %v", chunk.BlockId.String(), err)
		// }

		go func(wg *sync.WaitGroup, chunkFilePath string, chunk *chain.BlockInfo) {
			defer wg.Done()
			err = n.doDownloadChunk(chunkFilePath, chunk)
			if err != nil {
				log.Printf("chunk: %s download error: %v", chunk.BlockId.String(), err)
			}
			//TODO: retry use remain chunk
		}(&wg, chunkFilePath, &chunk)
	}
	wg.Wait()
	log.Printf("%d/%d chunk downloaded\n", minCountToDownload, len(fmeta.BlockInfo))
	return err
}

func (n Node) doDownloadChunk(chunkFilePath string, chunk *chain.BlockInfo) error {
	// fsta, err := os.Stat(chunkFilePath)
	// if err == nil {
	// 	if fsta.Size() != int64(chunk.ChunkSize) {
	// 		os.Remove(chunkFilePath)
	// 	} else {
	// 		return nil
	// 	}
	// }
	log.Println("chunkId:", chunk.BlockId.String())

	var start time.Time

	msg := utils.GetRandomcode(16)

	kr, _ := cesskeyring.FromURI(n.Confile.GetCtrlPrk(), cesskeyring.NetSubstrate{})
	// sign message
	sign, err := kr.Sign(kr.SigningContext([]byte(msg)))
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
	srv := NewClient(NewTcp(conTcp), n.FileStashDir, nil)

	start = time.Now()
	err = srv.RecvFile(filepath.Base(chunkFilePath), int64(chunk.BlockSize), n.Chain.GetPublicKey(), []byte(msg), sign[:])
	log.Printf("download chunk: %s cost: %dms \n", chunkFilePath, time.Since(start).Milliseconds())

	return err
}

func copyFile(src, dst string, length int64) error {
	srcfile, err := os.OpenFile(src, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer srcfile.Close()
	dstfile, err := os.OpenFile(src, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer dstfile.Close()

	var buf = make([]byte, 64*1024)
	var count int64
	for {
		n, err := srcfile.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		count += int64(n)
		if count < length {
			dstfile.Write(buf[:n])
		} else {
			tail := count - length
			if n >= int(tail) {
				dstfile.Write(buf[:(n - int(tail))])
			}
		}
	}

	return nil
}
