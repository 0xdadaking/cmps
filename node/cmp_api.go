package node

import (
	"cmp/configs"
	"cmp/node/resp"
	"cmp/pkg/chain"
	"cmp/pkg/utils"
	"net/http"

	"fmt"
	"log"
	"mime/multipart"
	"os"
	"path/filepath"

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

	ur, err := n.Upload(req.File, accountId)
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

	// local cache
	fpath := filepath.Join(n.FileStashDir, req.FileHash)
	// _, err := os.Stat(fpath)
	// if err == nil {
	// 	responseForFile(c, fpath, fid)
	// 	return
	// }

	// file meta info
	fmeta, err := n.Chain.GetFileMetaInfo(req.FileHash)
	if err != nil {
		if err.Error() == chain.ERR_Empty {
			resp.ErrorWithHttpStatus(c, err, 404)

		} else {
			resp.Error(c, err)
		}
		return
	}

	if string(fmeta.State) != chain.FILE_STATE_ACTIVE {
		resp.ErrorWithHttpStatus(c, fmt.Errorf("BackingUp"), 403)
		return
	}

	log.Printf("file meta: %v", fmeta)

	filename := string(fmeta.UserBriefs[0].File_name)
	log.Printf("file name: %v\n", filename)

	fdi, err := n.downloadFile(req.FileHash, &fmeta, n.FileStashDir)
	if err != nil {
		resp.Error(c, err)
		return
	}

	if fdi.ParityShards > 0 {
		fstat, err := os.Stat(fpath)
		if err != nil {
			resp.Error(c, err)
			return
		}
		if uint64(fstat.Size()) > uint64(fmeta.Size) {
			tempfile := fpath + ".temp"
			copyFile(fpath, tempfile, int64(fmeta.Size))
			os.Remove(fpath)
			os.Rename(tempfile, fpath)
		}
	}

	log.Println("restore finished, begin response file...")
	responseForFile(c, fpath, filename)
	return
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
