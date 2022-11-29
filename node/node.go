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
	"cmps/configs"
	"cmps/pkg/chain"
	"cmps/pkg/confile"
	"cmps/pkg/db"
	"cmps/pkg/logger"
	"log"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

type Oss interface {
	Run()
}

type Node struct {
	Confile      confile.Confiler
	Chain        chain.Chainer
	Logs         logger.Logger
	Cache        db.Cacher
	Gin          *gin.Engine
	FileStashDir string
	ChunksDir    string
}

// New is used to build a node instance
func New() *Node {
	return &Node{}
}

func (n *Node) Run() {
	gin.SetMode(gin.ReleaseMode)
	n.Gin = gin.Default()
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	config.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	config.AddAllowHeaders(
		configs.Header_Auth,
		configs.Header_Account,
		configs.Header_BucketName,
		"*",
	)
	n.Gin.Use(cors.New(config))

	n.Gin.Use(ginLoggerToConsole)
	// Add route
	n.addRoute()
	// Run
	log.Println("http server listener on", n.Confile.GetServicePort())
	n.Gin.Run(":" + n.Confile.GetServicePort())
}

func ginLoggerToConsole(c *gin.Context) {
	startTime := time.Now()               // start time
	c.Next()                              // Handling the Request
	endTime := time.Now()                 // end time
	latencyTime := endTime.Sub(startTime) // execution time
	reqMethod := c.Request.Method         // request method
	reqUri := c.Request.RequestURI        // required parameter
	statusCode := c.Writer.Status()       // status code
	clientIP := c.ClientIP()              // IP
	log.Printf(" %13v | %15s | %7s | %3d | %s \n",
		latencyTime,
		clientIP,
		reqMethod,
		statusCode,
		reqUri,
	)
}
