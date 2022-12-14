package filestash

import (
	"cmps/pkg/chain"
	"cmps/pkg/confile"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	cesskeyring "github.com/CESSProject/go-keyring"
	"github.com/pkg/errors"
)

type FileStash struct {
	fileStashDir string
	chunksDir    string
	keyring      *cesskeyring.KeyRing
	cessc        chain.Chainer

	relayHandlers       map[int64]*RelayHandler
	relayHandlerPutChan chan *RelayHandler
}

const (
	_FileStashDirName = "stashs"
	_DataFilename     = "data"
	_MetaFilename     = "meta.json"
)

var Must _must

type _must struct {
}

func (t _must) NewFileStash(parentDir string, cfg confile.Confiler, cessc chain.Chainer) *FileStash {
	fs, err := NewFileStash(parentDir, cfg, cessc)
	if err != nil {
		panic(err)
	}
	return fs
}

func NewFileStash(parentDir string, cfg confile.Confiler, cessc chain.Chainer) (*FileStash, error) {
	keyring, err := cesskeyring.FromURI(cfg.GetCtrlPrk(), cesskeyring.NetSubstrate{})
	if err != nil {
		return nil, err
	}

	fsd := filepath.Join(parentDir, _FileStashDirName)
	if err := os.MkdirAll(fsd, 0755); err != nil {
		return nil, err
	}
	ckd := filepath.Join(parentDir, _FileStashDirName, ".chunks")
	if err := os.MkdirAll(ckd, 0755); err != nil {
		return nil, err
	}
	fsth := &FileStash{
		fileStashDir:        fsd,
		chunksDir:           ckd,
		keyring:             keyring,
		cessc:               cessc,
		relayHandlers:       make(map[int64]*RelayHandler),
		relayHandlerPutChan: make(chan *RelayHandler),
	}
	startCleanCompleteRelayHandlerTask(fsth)
	return fsth, nil
}

func startCleanCompleteRelayHandlerTask(fsth *FileStash) {
	go func() {
		for {
			for k, rh := range fsth.relayHandlers {
				if !rh.completeTime.IsZero() && time.Now().After(rh.completeTime.Add(5*time.Second)) {
					rh.close()
					delete(fsth.relayHandlers, k)
					log.Printf("clean relay handler %p, %d", rh, rh.Id())
				}
				time.Sleep(time.Second)
			}
			time.Sleep(time.Second)
		}
	}()
}

func (t *FileStash) Dir() string { return t.fileStashDir }

type FileBriefInfo struct {
	OriginName string
	FileHash   string
	FilePath   string
	Size       int64
}

type SimpleFileMeta struct {
	FileHash   string `json:"fileHash"`
	OriginName string `json:"originName"`
}

func (t *FileStash) FileInfoByHash(fileHash string) (*FileBriefInfo, error) {
	dataFilename := filepath.Join(t.fileStashDir, fileHash, _DataFilename)
	fstat, err := os.Stat(dataFilename)
	if err != nil {
		return nil, err
	}
	r := FileBriefInfo{
		fileHash,
		fileHash,
		dataFilename,
		fstat.Size(),
	}
	sfm, err := t.loadSimpleFileMeta(fileHash)
	if sfm != nil && sfm.OriginName != "" {
		r.OriginName = sfm.OriginName
	}
	return &r, err
}

func (t *FileStash) loadSimpleFileMeta(fileHash string) (*SimpleFileMeta, error) {
	metabs, err := os.ReadFile(filepath.Join(t.fileStashDir, fileHash, _MetaFilename))
	if err != nil {
		return nil, err
	}

	var sfm SimpleFileMeta
	if err := json.Unmarshal(metabs, &sfm); err != nil {
		return nil, err
	}
	return &sfm, nil
}

func (t *FileStash) storeSimpleFileMeta(sfm *SimpleFileMeta) error {
	if sfm.FileHash == "" {
		return errors.New("fileHash field must not be empty")
	}
	bytes, err := json.Marshal(sfm)
	if err != nil {
		return err
	}
	metaFilename := filepath.Join(t.fileStashDir, sfm.FileHash, _MetaFilename)
	if err := os.WriteFile(metaFilename, bytes, os.ModePerm); err != nil {
		return err
	}
	return nil
}

func (t *FileStash) ensureFileHashDir(fileHash string) (string, error) {
	fileHashDir := filepath.Join(t.fileStashDir, fileHash)
	if _, err := os.Stat(fileHashDir); os.IsNotExist(err) {
		err = os.Mkdir(fileHashDir, 0755)
		if err != nil {
			return "", errors.Wrap(err, "make filehash dir error")
		}
	}
	return fileHashDir, nil
}

func (t *FileStash) createFileHashDataFile(fileHash string) (*os.File, error) {
	dir, err := t.ensureFileHashDir(fileHash)
	if err != nil {
		return nil, err
	}
	return os.Create(filepath.Join(dir, "data"))
}

func (t *FileStash) DownloadFile(fileHash string) (*FileBriefInfo, error) {
	fmeta, err := t.cessc.GetFileMetaInfo(fileHash)
	if err != nil {
		return nil, err
	}
	log.Printf("file meta: %v", fmeta)

	if string(fmeta.State) != chain.FILE_STATE_ACTIVE {
		return nil, errors.New("BackingUp")
	}

	return t.downloadFile(fileHash, &fmeta)
}
