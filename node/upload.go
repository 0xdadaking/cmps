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
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"os"
	"path/filepath"
	"time"

	"cmp/configs"
	"cmp/pkg/chain"
	"cmp/pkg/erasure"
	"cmp/pkg/hashtree"
	"cmp/pkg/utils"

	"github.com/pkg/errors"

	cesskeyring "github.com/CESSProject/go-keyring"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
)

type UploadResult struct {
	FileHash string
	TxHash   string
}

func (n *Node) Upload(file *multipart.FileHeader, accountId types.AccountID) (*UploadResult, error) {
	mpFile, err := file.Open()
	if err != nil {
		return nil, err
	}
	ccr, err := n.cutToChunks(mpFile, file.Size, accountId)
	if err != nil {
		return nil, err
	}
	dr, err := n.declareFileIfAbsent(accountId, ccr.fileHash, file.Filename)
	if err != nil {
		return nil, err
	}
	if dr.needRelay {
		ur := &UploadRelay{
			n,
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

func (n Node) declareFileIfAbsent(accountId types.AccountID, fileHash string, originFilename string) (*declareFileResult, error) {
	fmeta, err := n.Chain.GetFileMetaInfo(fileHash)
	if err != nil {
		if err.Error() != chain.ERR_Empty {
			return nil, err
		}
		userBrief := chain.UserBrief{
			User:        accountId,
			File_name:   types.Bytes(originFilename),
			Bucket_name: types.Bytes(configs.DEFAULT_BUCKET),
		}
		txHash, err := n.Chain.DeclarationFile(fileHash, userBrief)
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

func (n *Node) cutToChunks(fileInput io.ReadCloser, size int64, accountId types.AccountID) (*chunkCutResult, error) {
	chunkDir, err := os.MkdirTemp(n.ChunksDir, "up-chunks-")
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
	n             *Node
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

	// var existFile = make([]string, 0)
	// for i := 0; i < len(fpaths); i++ {
	// 	_, err := os.Stat(filepath.Join(n.FileDir, fpaths[i]))
	// 	if err != nil {
	// 		continue
	// 	}
	// 	existFile = append(existFile, fpaths[i])
	// }

	msg := utils.GetRandomcode(16)

	kr, err := cesskeyring.FromURI(t.n.Confile.GetCtrlPrk(), cesskeyring.NetSubstrate{})
	if err != nil {
		log.Println("get keyring error:", err)
		ch <- US_FAILED
		return
	}
	// sign message
	sign, err := kr.Sign(kr.SigningContext([]byte(msg)))
	if err != nil {
		log.Println("sign msg error:", err)
		ch <- US_FAILED
		return
	}

	// Get all scheduler
	schds, err := t.n.Chain.GetSchedulerList()
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
		srv := NewClient(NewTcp(conTcp), t.n.FileStashDir, t.chunkPaths)
		err = srv.SendFile(t.fileHash, t.fileSize, t.n.Chain.GetPublicKey(), []byte(msg), sign[:])
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
