package filestash

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"os"
	"path/filepath"
	"sort"
	"time"

	"cmps/configs"
	"cmps/pkg/cessfc"
	"cmps/pkg/chain"
	"cmps/pkg/erasure"
	"cmps/pkg/hashtree"
	"cmps/pkg/utils"

	cesskeyring "github.com/CESSProject/go-keyring"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"

	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
)

const (
	_FINISH_STEP = "finish"
	_ABORT_STEP  = "abort"
)

var (
	ErrFileOnPending = errors.New("the file of uploaded is pending")
)

type HandleStep struct {
	Step  string
	Msg   string
	Data  any
	Error error
}

func (t HandleStep) IsComplete() bool { return t.Step == _FINISH_STEP }
func (t HandleStep) IsAbort() bool    { return t.Error != nil }
func (t HandleStep) String() string {
	b, err := json.Marshal(t)
	if err != nil {
		return fmt.Sprintf("step:%s <json marshal error:%v>", t.Step, err)
	}
	return fmt.Sprint("progress", string(b))
}
func (t HandleStep) MarshalJSON() ([]byte, error) {
	type tmpType struct {
		Step  string `json:"step,omitempty"`
		Msg   string `json:"msg,omitempty"`
		Data  any    `json:"data,omitempty"`
		Error string `json:"error,omitempty"`
	}
	tt := tmpType{
		Step: t.Step,
		Msg:  t.Msg,
		Data: t.Data,
	}
	if t.Error != nil {
		tt.Error = t.Error.Error()
	}
	return json.Marshal(&tt)
}

type UploadResult struct {
	UploadId int64 `json:"uploadId"`
}

func (t *FileStash) Upload(fileHeader *multipart.FileHeader, accountId types.AccountID, forceUploadIfPending bool) (*UploadResult, error) {
	uploadId := time.Now().UnixMicro()
	_, ok := t.relayHandlers[uploadId]
	if ok {
		return &UploadResult{uploadId}, nil
	}

	rh := &RelayHandler{
		id:        uploadId,
		fileStash: t,
	}
	log.Printf("allot relay handler %p for %d", rh, uploadId)
	t.relayHandlers[uploadId] = rh
	go func() {
		t.relayHandlerPutChan <- rh
	}()

	openedMpFile, err := fileHeader.Open()
	if err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("internal error: %+v", r)
			}
		}()
		rh.Relay(openedMpFile, fileHeader, accountId, forceUploadIfPending)
	}()

	return &UploadResult{uploadId}, nil
}

func (t *FileStash) GetRelayHandler(uploadId int64) (*RelayHandler, error) {
	rh, ok := t.relayHandlers[uploadId]
	if !ok {
		return nil, errors.New("relay handler not exists for upload")
	}
	return rh, nil
}

func (t *FileStash) AnyRelayHandler() <-chan *RelayHandler {
	return t.relayHandlerPutChan
}

func cleanChunks(chunkDir string) {
	err := os.RemoveAll(chunkDir)
	if err != nil {
		log.Println("remove chunk dir error:", err)
	}
}

type RelayState struct {
	FileHash     string        `json:"fileHash,omitempty"`
	Miners       []string      `json:"miners,omitempty"`
	Steps        []*HandleStep `json:"steps,omitempty"`
	CompleteTime time.Time     `json:"completeTime,omitempty"`
}

func (t *RelayState) pushStep(step *HandleStep) {
	t.Steps = append(t.Steps, step)
	if step.Step == _FINISH_STEP || step.Step == _ABORT_STEP {
		t.CompleteTime = time.Now()
	}
}

func (t *RelayState) isCompleteBefore(ref time.Time, d time.Duration) bool {
	return !t.CompleteTime.IsZero() && ref.After(t.CompleteTime.Add(d))
}

type RelayHandler struct {
	id        int64
	fileStash *FileStash
	state     *RelayState
	stepChan  chan HandleStep
}

func (t *RelayHandler) Id() int64 { return t.id }

func (t *RelayHandler) ListenerProgress() <-chan HandleStep {
	if t.stepChan == nil {
		t.stepChan = make(chan HandleStep)
	}
	return t.stepChan
}

func (t *RelayHandler) State() *RelayState {
	return t.state
}

func (t *RelayHandler) CanClean() bool {
	return t.state != nil && t.state.isCompleteBefore(time.Now(), 1800*time.Second)
}

func (t *RelayHandler) pushStep(step string, arg ...any) {
	p := HandleStep{Step: step}
	if arg != nil {
		if err, ok := arg[0].(error); ok {
			p.Error = err
		} else if msg, ok := arg[0].(string); ok {
			p.Msg = msg
		} else {
			p.Data = arg[0]
		}
	}
	t.state.pushStep(&p)
	log.Output(2, fmt.Sprintf("%s", p))
	if t.stepChan != nil {
		t.stepChan <- p
	}
}

func (t *RelayHandler) close() {
	if t.stepChan != nil {
		close(t.stepChan)
	}
}

func (t *RelayHandler) stashFile(fileHash string, src io.Reader, originName string) error {
	fhdf, err := t.fileStash.createFileHashDataFile(fileHash)
	if err != nil {
		return err
	}
	if _, err := io.Copy(fhdf, src); err != nil {
		return err
	}

	return t.fileStash.storeSimpleFileMeta(&SimpleFileMeta{
		FileHash:   fileHash,
		OriginName: originName,
	})
}

func (t *RelayHandler) Relay(openedMpFile multipart.File, fileHeader *multipart.FileHeader, accountId types.AccountID, forceUploadIfPending bool) (retErr error) {
	defer func() {
		if retErr != nil {
			t.pushStep(_ABORT_STEP, retErr)
		}
	}()
	defer openedMpFile.Close()

	t.state = &RelayState{}

	t.pushStep("sharding")
	ccr, err := t.cutToChunks(openedMpFile, fileHeader.Size, accountId)
	if err != nil {
		return errors.Wrap(err, "shard file error")
	}
	defer cleanChunks(ccr.chunkDir)
	t.state.FileHash = ccr.fileHash
	t.pushStep("sharded", map[string]string{"fileHash": ccr.fileHash})

	t.pushStep("stashing")
	_, err = openedMpFile.Seek(0, io.SeekStart)
	if err != nil {
		return errors.Wrap(err, "seek file error when stash")
	}
	err = t.stashFile(ccr.fileHash, openedMpFile, fileHeader.Filename)
	if err != nil {
		return errors.Wrap(err, "stash file error")
	}

	t.pushStep("bucketing")
	if _, err := t.createBucketIfAbsent(accountId); err != nil {
		return errors.Wrap(err, "create bucket error")
	}

	t.pushStep("declaring")
	dr, err := t.declareFileIfAbsent(accountId, ccr.fileHash, fileHeader.Filename)
	if err != nil {
		if forceUploadIfPending && !errors.Is(err, ErrFileOnPending) {
			return errors.Wrap(err, "declare file error")
		}
	}
	if dr.needRelay {
		err := t.relayUpload(&RelayContext{
			ccr.fileHash,
			fileHeader.Filename,
			dr.txHash,
			fileHeader.Size,
			ccr.chunkDir,
			ccr.chunkPaths,
			accountId,
		})
		if err == nil {
			t.blockPollMinerStorageState(ccr.fileHash)
		}
	} else {
		miners, err := queryFileStoredMiners(ccr.fileHash, t.fileStash.cessc)
		if err != nil {
			t.pushStep("thunder", map[string]string{"error": err.Error()})
		} else {
			t.state.Miners = miners
			t.pushStep("thunder", makeMinersForProgress(miners))
		}
	}
	t.pushStep(_FINISH_STEP)
	return nil
}

func queryFileStoredMiners(fileHash string, cessc chain.Chainer) ([]string, error) {
	var err error
	fmeta, err := cessc.GetFileMetaInfo(fileHash)
	if err != nil {
		return nil, err
	}
	return extractMiners(&fmeta), nil
}

func extractMiners(fmeta *chain.FileMetaInfo) []string {
	var err error
	miners := make([]string, len(fmeta.BlockInfo))
	for i, b := range fmeta.BlockInfo {
		miners[i], err = utils.EncodePublicKeyAsCessAccount(b.MinerAcc[:])
		if err != nil {
			log.Println("encode account error:", err)
			continue
		}
	}
	return miners
}

func (t *RelayHandler) createBucketIfAbsent(accountId types.AccountID) (string, error) {
	_, err := t.fileStash.cessc.GetBucketInfo(accountId[:], configs.DEFAULT_BUCKET)
	if err != nil {
		log.Println("get bucket info error:", err)
		txHash, err := t.fileStash.cessc.CreateBucket(accountId[:], configs.DEFAULT_BUCKET)
		log.Println("create bucket tx:", txHash)
		return txHash, err
	}
	return "", nil
}

type declareFileResult struct {
	needRelay bool
	txHash    string
}

func (t *RelayHandler) declareFileIfAbsent(accountId types.AccountID, fileHash string, originFilename string) (*declareFileResult, error) {
	fmeta, err := t.fileStash.cessc.GetFileMetaInfo(fileHash)
	if err != nil {
		if err.Error() != chain.ERR_Empty {
			return nil, err
		}
		userBrief := chain.UserBrief{
			User:        accountId,
			File_name:   types.Bytes(originFilename),
			Bucket_name: types.Bytes(configs.DEFAULT_BUCKET),
		}
		txHash, err := t.fileStash.cessc.DeclarationFile(fileHash, userBrief)
		if err != nil {
			return nil, errors.Wrap(err, "make declare file transaction failed")
		}
		return &declareFileResult{true, txHash}, nil
	}
	if string(fmeta.State) == chain.FILE_STATE_ACTIVE {
		return &declareFileResult{false, ""}, nil
	}
	return &declareFileResult{true, ""}, ErrFileOnPending
}

type chunkCutResult struct {
	chunkDir   string
	chunkPaths []string
	fileHash   string
}

func (t *RelayHandler) cutToChunks(fileInput io.ReadCloser, size int64, accountId types.AccountID) (*chunkCutResult, error) {
	chunkDir, err := os.MkdirTemp(t.fileStash.chunksDir, "up-chunks-")
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
		newPath := filepath.Join(chunkDir, fmt.Sprintf("%s.%03d", fileHash, i))
		if err := os.Rename(cp.Name(), newPath); err != nil {
			return nil, errors.Wrap(err, "rename chunk file error")
		}
		chunkPaths[i] = newPath
	}
	return &chunkCutResult{chunkDir, chunkPaths, fileHash}, nil
}

type RelayContext struct {
	fileHash      string
	filename      string
	declareTxHash string
	fileSize      int64
	chunkDir      string
	chunkPaths    []string
	accountId     types.AccountID
}

func (t *RelayHandler) relayUpload(rctx *RelayContext) (retErr error) {
	msg, sign, err := makeSign(t.fileStash.keyring)
	if err != nil {
		return errors.Wrap(err, "sign msg error")
	}
	incSleep := func(i int, msg string) {
		n := i*i + 2
		t.pushStep("uploading", fmt.Sprintf("%s, %d seconds later try again", msg, n))
		time.Sleep(time.Duration(n) * time.Second)
	}
	for i := 0; i < 3; i++ {
		scheds, err := randomSequenceScheduler(t.fileStash.cessc)
		if err != nil {
			incSleep(i, "query scheduler server failed")
			continue
		}
		_, err = t.tryUpload(rctx, msg, sign, scheds)
		if err != nil {
			incSleep(i, "poll scheduler servers failed")
			continue
		}
		return nil
	}
	return errors.New("final upload failed")
}

func makeSign(keyring *cesskeyring.KeyRing) (string, []byte, error) {
	msg := utils.GetRandomcode(16)
	sign, err := keyring.Sign(keyring.SigningContext([]byte(msg)))
	if err != nil {
		return "", nil, err
	}
	return msg, sign[:], nil
}

func randomSequenceScheduler(cessc chain.Chainer) ([]chain.SchedulerInfo, error) {
	scheds, err := cessc.GetSchedulerList()
	if err != nil {
		return nil, err
	}
	utils.RandSlice(scheds)
	return scheds, nil
}

func (t *RelayHandler) tryUpload(rctx *RelayContext, msg string, sign []byte, scheds []chain.SchedulerInfo) (int, error) {
	upload := func(address string) (err error) {
		log.Printf("begin dial to %s ...", address)
		conn, err := cessfc.Dial(address, time.Second*5)
		if err != nil {
			return errors.Wrapf(err, "dial address %s failed", address)
		}

		t.pushStep("uploading", fmt.Sprintf("connected to scheduler server: %s", address))
		client := cessfc.NewClient(conn, t.fileStash.fileStashDir, rctx.chunkPaths)
		client.SetFsiReceiver(t)

		pubKey := t.fileStash.keyring.Public()
		err = client.SendFile(rctx.fileHash, rctx.fileSize, pubKey[:], []byte(msg), sign[:])
		if err != nil {
			return errors.Wrapf(err, "send file invoke error")
		}
		return nil
	}

	for i, sched := range scheds {
		if err := upload(sched.Ip.String()); err != nil {
			log.Printf("%v, try next", err)
			continue
		}
		return i, nil
	}
	return -1, errors.New("no available scheduler server")
}

func (t *RelayHandler) Receive(fsi *cessfc.FileStoreInfo) {
	if fsi.Miners != nil && len(fsi.Miners) > 0 {
		miners := make([]string, 0, len(fsi.Miners))
		for _, v := range fsi.Miners {
			miners = append(miners, v)
		}
		sort.Slice(miners, func(i, j int) bool {
			return miners[i] > miners[j]
		})
		if !slices.Equal(t.state.Miners, miners) {
			t.state.Miners = miners
			t.pushStep("uploading", makeMinersForProgress(miners))
		}
	}
}

func makeMinersForProgress(miners []string) map[string][]string {
	return map[string][]string{"miners": miners}
}

func (t *RelayHandler) blockPollMinerStorageState(fileHash string) {
	t.pushStep("storing", "begin polling the file storage state on chain")
	for i := 0; i < 30; i++ {
		fmeta, err := t.fileStash.cessc.GetFileMetaInfo(fileHash)
		if err != nil {
			log.Println("poll file state error:", err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("polling %s, %s, %d", fileHash, string(fmeta.State), i)
		if string(fmeta.State) == chain.FILE_STATE_ACTIVE {
			miners := extractMiners(&fmeta)
			sort.Slice(miners, func(i, j int) bool {
				return miners[i] > miners[j]
			})
			t.state.Miners = miners
			t.pushStep("stored", makeMinersForProgress(miners))
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.pushStep("storing", "the file storage may be processing, try poll later")
}
