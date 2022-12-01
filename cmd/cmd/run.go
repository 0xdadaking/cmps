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
	"time"

	"cmps/api"
	"cmps/filestash"
	"cmps/pkg/chain"
	"cmps/pkg/confile"

	"github.com/spf13/cobra"
)

// account
const (
	// CESS token precision
	CESSTokenPrecision = 1_000_000_000_000
	// MinimumBalance is the minimum balance required for the program to run
	// The unit is pico
	MinimumBalance = 2 * CESSTokenPrecision
	// BlockInterval is the time interval for generating blocks, in seconds
	BlockInterval = time.Second * time.Duration(6)
	// Time out waiting for transaction completion
	TimeOut_WaitBlock = time.Duration(time.Second * 15)
)

// explanation
const (
	HELP_common = `Please check with the following help information:
    1.Check if the wallet balance is sufficient
    2.Block hash:`
	HELP_register = `    3.Check the FileMap.OssRegister transaction event result in the block hash above:
        If system.ExtrinsicFailed is prompted, it means failure;
        If system.ExtrinsicSuccess is prompted, it means success;`
	HELP_update = `    3.Check the FileMap.OssUpdate transaction event result in the block hash above:
        If system.ExtrinsicFailed is prompted, it means failure;
        If system.ExtrinsicSuccess is prompted, it means success;`
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// start service
func Command_Run_Runfunc(cmd *cobra.Command, _ []string) {
	var err error

	// Building Profile Instances
	cfg, err := buildConfigFile(cmd)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	n, err := api.NewNode(cfg)

	//Build chain instance
	n.Chain, err = buildChain(n.Confile, TimeOut_WaitBlock)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	//Build Data Directory
	err = buildDir(n)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	fsth, err := filestash.NewFileStash(n.Confile.GetDataDir(), cfg, n.Chain)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	n.FileStash = fsth

	// run
	n.Run()
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

	if accountinfo.Data.Free.CmpAbs(new(big.Int).SetUint64(MinimumBalance)) == -1 {
		return nil, fmt.Errorf("Account balance is less than %v pico\n", MinimumBalance)
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
		time.Sleep(time.Second * BlockInterval)
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
				msg := HELP_common + fmt.Sprintf(" %v\n", txhash)
				msg += HELP_register
				return fmt.Errorf("[pending] %v\n", msg)
			}
			return err
		}
	}
	return nil
}

func buildDir(n *api.Node) error {
	baseDir := n.Confile.GetDataDir()

	_, err := os.Stat(baseDir)
	if err != nil {
		err = os.MkdirAll(baseDir, 0755)
		if err != nil {
			return err
		}
	}

	log.Println(baseDir)
	return nil
}
