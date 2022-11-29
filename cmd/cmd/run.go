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
package cmd

import (
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"cmps/configs"
	"cmps/node"
	"cmps/pkg/chain"
	"cmps/pkg/confile"
	"cmps/pkg/db"
	"cmps/pkg/utils"

	"github.com/spf13/cobra"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// start service
func Command_Run_Runfunc(cmd *cobra.Command, _ []string) {
	var (
		err  error
		node = node.New()
	)

	// Building Profile Instances
	node.Confile, err = buildConfigFile(cmd)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	//Build chain instance
	node.Chain, err = buildChain(node.Confile, configs.TimeOut_WaitBlock)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	//Build Data Directory
	err = buildDir(node)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// run
	node.Run()
}

func buildConfigFile(cmd *cobra.Command) (confile.Confiler, error) {
	var conFilePath string
	configpath1, _ := cmd.Flags().GetString("config")
	configpath2, _ := cmd.Flags().GetString("c")
	if configpath1 != "" {
		conFilePath = configpath1
	} else {
		conFilePath = configpath2
	}

	cfg := confile.NewConfigfile()
	if err := cfg.Parse(conFilePath); err != nil {
		return nil, err
	}
	return cfg, nil
}

func buildChain(cfg confile.Confiler, timeout time.Duration) (chain.Chainer, error) {
	// connecting chain
	client, err := chain.NewChainClient(cfg.GetRpcAddr(), cfg.GetCtrlPrk(), timeout)
	if err != nil {
		return nil, err
	}

	// judge the balance
	accountinfo, err := client.GetAccountInfo(client.GetPublicKey())
	if err != nil {
		return nil, err
	}

	if accountinfo.Data.Free.CmpAbs(new(big.Int).SetUint64(configs.MinimumBalance)) == -1 {
		return nil, fmt.Errorf("Account balance is less than %v pico\n", configs.MinimumBalance)
	}

	// sync block
	for {
		ok, err := client.GetSyncStatus()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		log.Println("In sync block...")
		time.Sleep(time.Second * configs.BlockInterval)
	}
	log.Println("Complete synchronization of primary network block data")

	// whether to register
	ossStata, err := client.GetState(client.GetPublicKey())
	if err != nil && err.Error() != chain.ERR_RPC_EMPTY_VALUE.Error() {
		return nil, err
	}

	// register
	if ossStata == "" {
		if err := register(cfg, client); err != nil {
			return nil, err
		}
	}
	return client, nil
}

func register(cfg confile.Confiler, client chain.Chainer) error {
	txhash, err := client.Register(cfg.GetServiceAddr(), cfg.GetServicePort())
	if err != nil {
		if err.Error() == chain.ERR_RPC_EMPTY_VALUE.Error() {
			return fmt.Errorf("[err] Please check your wallet balance")
		} else {
			if txhash != "" {
				msg := configs.HELP_common + fmt.Sprintf(" %v\n", txhash)
				msg += configs.HELP_register
				return fmt.Errorf("[pending] %v\n", msg)
			}
			return err
		}
	}
	return nil
}

func buildDir(n *node.Node) error {
	baseDir := filepath.Join(n.Confile.GetDataDir(), configs.BaseDir)

	_, err := os.Stat(baseDir)
	if err != nil {
		err = os.MkdirAll(baseDir, 0755)
		if err != nil {
			return err
		}
	}

	cacheDir := filepath.Join(baseDir, configs.Cache)
	//os.RemoveAll(cacheDir)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return err
	}
	//Build cache instance
	n.Cache, err = buildCache(cacheDir)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	n.FileStashDir = filepath.Join(baseDir, configs.FileStashs)
	//os.RemoveAll(fileDir)
	if err := os.MkdirAll(n.FileStashDir, 0755); err != nil {
		return err
	}

	n.ChunksDir = filepath.Join(baseDir, configs.Chunks)
	//os.RemoveAll(fileDir)
	if err := os.MkdirAll(n.ChunksDir, 0755); err != nil {
		return err
	}

	log.Println(baseDir)
	return nil
}

func buildCache(cacheDir string) (db.Cacher, error) {
	cache, err := db.NewCache(cacheDir, 0, 0, configs.NameSpace)
	if err != nil {
		return nil, err
	}

	ok, err := cache.Has([]byte("SigningKey"))
	if err != nil {
		return nil, err
	}
	if !ok {
		err = cache.Put([]byte("SigningKey"), []byte(utils.GetRandomcode(16)))
	}
	return cache, err
}
