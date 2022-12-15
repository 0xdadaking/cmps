package api

import (
	"cmps/api/resp"
	"cmps/configs"
	"cmps/filestash"
	"cmps/pkg/chain"
	"cmps/pkg/utils"
	"net/http"

	"fmt"
	"log"
	"mime/multipart"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
)

func (n Node) addRoute() {
	n.Gin.POST("/bucket", n.CreateBucket)
	n.Gin.GET("/file-state/:fileHash", n.GetFileState)
	n.Gin.PUT("/file", n.UploadFile)
	n.Gin.GET("/file/:fileHash", n.DownloadFile)
	n.Gin.DELETE("/file", n.DeleteFile)
	n.Gin.GET("/upload-progress/:uploadId", n.ListenerUploadProgress)
	n.Gin.GET("/upload-progress1", n.DebugUploadProgress)

	n.Gin.StaticFile("favicon.ico", "./static/favicon.ico")
	n.Gin.LoadHTMLFiles("./static/index.html")
	n.Gin.GET("/demo", func(c *gin.Context) {
		c.HTML(200, "index.html", nil)
	})
}

type (
	FileHashOnlyReq struct {
		FileHash string `json:"fileHash" form:"fileHash" uri:"fileHash" binding:"required"`
	}

	BucketCreateReq struct {
		WalletAddress string `json:"walletAddress" form:"walletAddress" binding:"required"`
	}

	FilePutReq struct {
		WalletAddress        string                `form:"walletAddress" binding:"required"`
		File                 *multipart.FileHeader `form:"file" binding:"required"`
		forceUploadIfPending bool                  `form:"force"`
	}

	FileDeleteReq struct {
		FileHashOnlyReq
		WalletAddress string `json:"walletAddress" form:"walletAddress" binding:"required"`
	}

	UploadStateReq struct {
		UploadId int64 `json:"uploadId" form:"uploadId" uri:"uploadId" binding:"required"`
	}
)

func (n Node) CreateBucket(c *gin.Context) {
	var req BucketCreateReq
	if err := c.ShouldBind(&req); err != nil {
		resp.Error(c, err)
		return
	}

	pubKey, err := utils.DecodePublicKeyOfCessAccount(req.WalletAddress)
	if err != nil {
		resp.Error(c, err)
		return
	}
	txHash, err := n.Chain.CreateBucket(pubKey, configs.DEFAULT_BUCKET)
	if err != nil {
		resp.Error(c, err)
		return
	}
	resp.Ok(c, map[string]string{"txHash:": txHash})

}

func (n Node) UploadFile(c *gin.Context) {
	var req FilePutReq
	if err := c.ShouldBind(&req); err != nil {
		resp.Error(c, err)
		return
	}

	accountId, err := utils.ToAccountIdByCessAddress(req.WalletAddress)
	if err != nil {
		resp.Error(c, err)
		return
	}

	ur, err := n.FileStash.Upload(req.File, accountId, req.forceUploadIfPending)
	if err != nil {
		resp.Error(c, err)
		return
	}
	resp.Ok(c, ur)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type LupMsgType int

const (
	LMT_STATE LupMsgType = 0 + iota
	LMT_PROGRESS
)

type LupMsg struct {
	Type LupMsgType `json:"type"`
	Msg  any        `json:"msg"`
}

func (n Node) ListenerUploadProgress(c *gin.Context) {
	log.Printf("upload progress listener coming, m:%s, url:%s, client ip:%s", c.Request.Method, c.Request.URL, c.ClientIP())
	var f UploadStateReq
	if err := c.ShouldBindUri(&f); err != nil {
		resp.Error(c, err)
		return
	}
	rh, err := n.FileStash.GetRelayHandler(f.UploadId)
	if err != nil {
		resp.ErrorWithHttpStatus(c, err, 404)
		return
	}

	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("websocket upgrade error: ", err)
		return
	}
	defer ws.Close()

	closeSignal := make(chan bool)
	ws.SetCloseHandler(func(code int, text string) error {
		log.Printf("websocket close code:%d, text:%s", code, text)
		closeSignal <- true
		return nil
	})
	pushMsgForRelayHandler(ws, rh, closeSignal)
	log.Printf("upload progress listener %d finish", f.UploadId)
}

func pushMsgForRelayHandler(ws *websocket.Conn, rh *filestash.RelayHandler, closeSignal <-chan bool) {
	err := ws.WriteJSON(LupMsg{LMT_STATE, rh.State()})
	if err != nil {
		log.Printf("write relay handler state to websocket error: %+v", err)
		return
	}
	for {
		select {
		case p := <-rh.ListenerProgress():
			err = ws.WriteJSON(LupMsg{LMT_PROGRESS, p})
			if err != nil {
				log.Printf("write relay handler progress to websocket error: %+v", err)
				return
			}
			if p.IsComplete() {
				return
			}
			if p.IsAbort() {
				log.Println("progress abort error:", p.Error)
				return
			}
		case <-closeSignal:
			log.Println("websocket close signal coming")
			return
		}
	}
}

func (n Node) DebugUploadProgress(c *gin.Context) {
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("websocket upgrade error: ", err)
		return
	}
	defer ws.Close()

	closeSignal := make(chan bool)
	wsClosed := false
	ws.SetCloseHandler(func(code int, text string) error {
		log.Printf("websocket close code:%d, text:%s", code, text)
		wsClosed = true
		closeSignal <- true
		return nil
	})
	ws.WriteMessage(websocket.TextMessage, []byte("waiting for file upload..."))
	for !wsClosed {
		rh := <-n.FileStash.AnyRelayHandler()
		pushMsgForRelayHandler(ws, rh, closeSignal)
	}
	log.Println("debug upload progress websocket listener finish")
}

func (n Node) GetFileState(c *gin.Context) {
	var req FileHashOnlyReq
	if err := c.ShouldBindUri(&req); err != nil {
		resp.Error(c, err)
		return
	}
	fmeta, err := n.Chain.GetFileMetaInfo(req.FileHash)
	if err != nil {
		if errors.Is(err, chain.ERR_RPC_EMPTY_VALUE) {
			resp.ErrorWithHttpStatus(c, err, 404)
		} else {
			resp.Error(c, err)
		}
		return
	}

	resp.Ok(c, fmeta)
}

func responseForFile(c *gin.Context, filepath, filename string) {
	c.Writer.Header().Add("Content-Disposition", fmt.Sprintf("attachment; filename=%v", filename))
	c.Writer.Header().Add("Content-Type", "application/octet-stream")
	c.File(filepath)
}

func (n Node) DownloadFile(c *gin.Context) {
	var req FileHashOnlyReq
	if err := c.ShouldBindUri(&req); err != nil {
		resp.Error(c, err)
		return
	}

	skipStash := c.Query("skipStash") != ""

	if !skipStash {
		fbi, err := n.FileStash.FileInfoByHash(req.FileHash)
		if fbi != nil && fbi.Size > 0 {
			responseForFile(c, fbi.FilePath, fbi.OriginName)
			return
		} else {
			log.Println(err)
		}
	}

	fbi, err := n.FileStash.DownloadFile(req.FileHash)
	if err != nil {
		resp.Error(c, err)
		return
	}

	log.Println("download finished, begin response file...", fbi.FilePath)
	responseForFile(c, fbi.FilePath, fbi.OriginName)
}

func (n Node) DeleteFile(c *gin.Context) {
	var req FileDeleteReq
	if err := c.ShouldBind(&req); err != nil {
		resp.Error(c, err)
		return
	}
	pkey, err := utils.DecodePublicKeyOfCessAccount(req.WalletAddress)
	if err != nil {
		resp.Error(c, err)
		return
	}
	txHash, err := n.Chain.DeleteFile(pkey, req.FileHash)
	if err != nil {
		resp.Error(c, err)
		return
	}
	n.FileStash.RemoveFile(req.FileHash)
	resp.Ok(c, txHash)
}
