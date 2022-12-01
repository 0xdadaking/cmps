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
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"os"
	"path/filepath"
	"time"

	"cmps/configs"
	"cmps/pkg/cessfc"
	"cmps/pkg/chain"
	"cmps/pkg/erasure"
	"cmps/pkg/hashtree"
	"cmps/pkg/utils"

	"github.com/pkg/errors"

	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
)

type UploadResult struct {
	FileHash string `json:"fileHash"`
	TxHash   string `json:"txHash,omitempty"`
}

func (t *FileStash) Upload(file *multipart.FileHeader, accountId types.AccountID) (*UploadResult, error) {
	mpFile, err := file.Open()
	if err != nil {
		return nil, err
	}
	ccr, err := t.cutToChunks(mpFile, file.Size, accountId)
	if err != nil {
		return nil, err
	}
	dr, err := t.declareFileIfAbsent(accountId, ccr.fileHash, file.Filename)
	if err != nil {
		return nil, err
	}
	if dr.needRelay {
		ur := &UploadRelay{
			t,
			ccr.fileHash,
			file.Filename,
			dr.txHash,
			file.Size,
			ccr.chunkPaths,
			accountId,
		}
		go ur.relayUpload()
	} else {
		go cleanChunks(ccr)
	}
	return &UploadResult{ccr.fileHash, dr.txHash}, nil
}

func cleanChunks(ccr *chunkCutResult) {
	err := os.RemoveAll(ccr.chunkDir)
	if err != nil {
		log.Println("remove chunk dir error:", err)
	}
}

type declareFileResult struct {
	needRelay bool
	txHash    string
}

func (t *FileStash) declareFileIfAbsent(accountId types.AccountID, fileHash string, originFilename string) (*declareFileResult, error) {
	fmeta, err := t.cessc.GetFileMetaInfo(fileHash)
	if err != nil {
		if err.Error() != chain.ERR_Empty {
			return nil, err
		}
		userBrief := chain.UserBrief{
			User:        accountId,
			File_name:   types.Bytes(originFilename),
			Bucket_name: types.Bytes(configs.DEFAULT_BUCKET),
		}
		txHash, err := t.cessc.DeclarationFile(fileHash, userBrief)
		if err != nil {
			return nil, errors.Wrap(err, "make declare file transaction failed")
		}
		return &declareFileResult{true, txHash}, nil
	}
	if string(fmeta.State) == chain.FILE_STATE_ACTIVE {
		return &declareFileResult{false, ""}, nil
	}
	return nil, errors.Errorf("upload ignore, the file is already %s", string(fmeta.State))
}

type chunkCutResult struct {
	chunkDir   string
	chunkPaths []string
	fileHash   string
}

func (t *FileStash) cutToChunks(fileInput io.ReadCloser, size int64, accountId types.AccountID) (*chunkCutResult, error) {
	chunkDir, err := os.MkdirTemp(t.chunksDir, "up-chunks-")
	if err != nil {
		return nil, err
	}

	var cps []*os.File
	shardCreater := func(index int) (io.WriteCloser, error) {
		f, err := os.Create(filepath.Join(chunkDir, fmt.Sprintf("chunk-%d", index)))
		if err != nil {
			return nil, err
		}
		cps = append(cps, f)
		return f, nil
	}
	shardOpener := func(index int) (io.ReadCloser, int64, error) {
		f, err := os.Open(filepath.Join(chunkDir, fmt.Sprintf("chunk-%d", index)))
		if err != nil {
			return nil, 0, err
		}
		fstat, err := f.Stat()
		if err != nil {
			return nil, 0, err
		}
		return f, fstat.Size(), nil
	}
	rsece := erasure.NewRsecEncoder(fileInput, size, shardCreater, shardOpener)
	err = rsece.Encode()
	if err != nil {
		return nil, err
	}

	// Calc merkle hash tree
	shardFilenames := make([]string, len(cps))
	for i, cp := range cps {
		shardFilenames[i] = cp.Name()
	}
	hTree, err := hashtree.NewFromShardFilenames(shardFilenames)
	if err != nil {
		return nil, errors.Wrap(err, "new merkle hash tree faild")
	}

	// Merkel root hash
	fileHash := hex.EncodeToString(hTree.MerkleRoot())

	// Rename the file and chunks with root hash
	chunkPaths := make([]string, len(cps))
	for i, cp := range cps {
		chunkPaths[i] = cp.Name()
	}
	return &chunkCutResult{chunkDir, chunkPaths, fileHash}, nil
}

type UploadRelay struct {
	fileStash     *FileStash
	fileHash      string
	filename      string
	declareTxHash string
	fileSize      int64
	chunkPaths    []string
	accountId     types.AccountID
}

type UploadSignal byte

const (
	US_RETRY = UploadSignal(1) + iota
	US_OK
	US_FAILED
)

func (t UploadRelay) relayUpload() {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	var chUs = make(chan UploadSignal, 1)

	go t.doUpload(chUs)
	for {
		select {
		case result := <-chUs:
			if result == US_RETRY {
				go t.doUpload(chUs)
				time.Sleep(time.Second * 6)
			}
			if result == US_OK {
				log.Printf("File [%v] relay upload successfully\n", t.fileHash)
				return
			}
			if result == US_FAILED {
				log.Printf("File [%v] relay upload failed\n", t.fileHash)
				return
			}
		}
	}
}

func (t UploadRelay) doUpload(ch chan UploadSignal) {
	defer func() {
		err := recover()
		if err != nil {
			ch <- 1
			log.Println(err)
		}
	}()

	// sign message
	kr := t.fileStash.keyring
	msg := utils.GetRandomcode(16)
	sign, err := kr.Sign(kr.SigningContext([]byte(msg)))
	if err != nil {
		log.Println("sign msg error:", err)
		ch <- US_FAILED
		return
	}

	// Get all scheduler
	schds, err := t.fileStash.cessc.GetSchedulerList()
	if err != nil {
		log.Println("fetch scheduler error:", err, "retry again")
		ch <- US_RETRY
		return
	}

	utils.RandSlice(schds)

	for _, schd := range schds {
		tcpAddr, err := net.ResolveTCPAddr("tcp", schd.Ip.String())
		if err != nil {
			log.Println("resolve tcp address error", err, "for", schd.Ip.String())
			continue
		}
		netConn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second*5)
		if err != nil {
			log.Println("dial address", tcpAddr, "error:", err, "try next")
			continue
		}

		conTcp, ok := netConn.(*net.TCPConn)
		if !ok {
			//should not be occurse
			continue
		}

		log.Println("connect to ", tcpAddr)
		srv := cessfc.NewClient(cessfc.NewTcp(conTcp), t.fileStash.Dir(), t.chunkPaths)
		pubKey := t.fileStash.keyring.Public()
		err = srv.SendFile(t.fileHash, t.fileSize, pubKey[:], []byte(msg), sign[:])
		if err != nil {
			log.Panicln("send file error:", err)
			continue
		}
		ch <- US_OK
		log.Panicln("send files finish")
		return
	}
	ch <- US_RETRY
}
