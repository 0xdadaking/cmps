package api

import (
	"cmps/api/resp"
	"cmps/configs"
	"cmps/pkg/chain"
	"cmps/pkg/utils"
	"net/http"

	"fmt"
	"log"
	"mime/multipart"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

func (n Node) addRoute() {
	n.Gin.POST("/bucket", n.CreateBucket)
	n.Gin.GET("/file-state/:fileHash", n.GetFileState)
	n.Gin.PUT("/file", n.UploadFile)
	n.Gin.GET("/file/:fileHash", n.DownloadFile)
	n.Gin.DELETE("/file", n.DeleteFile)

	n.Gin.StaticFile("favicon.ico", "./static/favicon.ico")
	n.Gin.StaticFS("demo/", http.Dir("static/"))
}

type (
	FileHashOnlyReq struct {
		FileHash string `json:"fileHash" form:"fileHash" uri:"fileHash" binding:"required"`
	}

	BucketCreateReq struct {
		//Name          string `json:"name" form:"name" binding:"required"`
		WalletAddress string `json:"walletAddress" form:"walletAddress" binding:"required"`
	}

	FilePutReq struct {
		WalletAddress string `form:"walletAddress" binding:"required"`
		//Bucket        string                `form:"bucket" binding:"required"`
		File *multipart.FileHeader `form:"file" binding:"required"`
	}

	FileDeleteReq struct {
		FileHashOnlyReq
		WalletAddress string `form:"walletAddress" binding:"required"`
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

	ur, err := n.FileStash.Upload(req.File, accountId)
	if err != nil {
		resp.Error(c, err)
		return
	}
	resp.Ok(c, ur)
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
	resp.Ok(c, txHash)
}
