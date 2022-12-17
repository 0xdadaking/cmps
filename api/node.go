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

package api

import (
	"cmps/filestash"
	"cmps/pkg/chain"
	"cmps/pkg/confile"
	"log"

	"github.com/centrifuge/go-substrate-rpc-client/v4/signature"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

type Node struct {
	Confile   confile.Confiler
	Chain     chain.Chainer
	Gin       *gin.Engine
	FileStash *filestash.FileStash
	keyring   signature.KeyringPair
}

// NewNode is used to build a node instance
func NewNode(cfg confile.Confiler) (*Node, error) {
	var n Node
	keyring, err := signature.KeyringPairFromSecret(cfg.GetCtrlPrk(), 0)
	if err != nil {
		return nil, err
	}
	n.keyring = keyring
	n.Confile = cfg
	return &n, nil
}

func MustNewNode(cfg confile.Confiler) *Node {
	var n Node
	keyring, err := signature.KeyringPairFromSecret(cfg.GetCtrlPrk(), 0)
	if err != nil {
		panic(err)
	}
	n.keyring = keyring
	n.Confile = cfg
	return &n
}

func (n *Node) Run() {
	gin.SetMode(gin.ReleaseMode)
	n.Gin = gin.Default()
	n.Gin.Use(cors.Default())

	// Add route
	n.addRoute()
	// Run
	log.Println("http server listener on", n.Confile.GetServicePort())
	n.Gin.Run(":" + n.Confile.GetServicePort())
}
