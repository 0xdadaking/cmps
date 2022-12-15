package cessfc

import (
	"cmps/pkg/chain"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

type ConMgr struct {
	conn     *TcpCon
	dir      string
	fileName string

	sendFiles []string

	waitNotify chan bool
	stop       chan struct{}

	fsiReceiver FileStoreInfoReceiver
	stopFsiPoll bool
}

func (c *ConMgr) handler() error {
	var (
		err      error
		recvFile *os.File
	)

	defer func() {
		//recover()
		c.conn.Close()
		close(c.waitNotify)
		if recvFile != nil {
			_ = recvFile.Close()
		}
	}()

	for !c.conn.IsClose() {
		m, ok := c.conn.GetMsg()
		if !ok {
			return fmt.Errorf("Getmsg failed")
		}
		if m == nil {
			continue
		}

		switch m.MsgType {
		case MsgHead:
			switch cap(m.Bytes) {
			case TCP_ReadBuffer:
				readBufPool.Put(m.Bytes)
			default:
			}
			c.conn.SendMsg(NewNotifyMsg(c.fileName, Status_Ok))

		case MsgFileSt:
			if m.Bytes != nil && len(m.Bytes) > 0 {
				var fsi FileStoreInfo
				err := json.Unmarshal(m.Bytes[:m.FileSize], &fsi)
				if err != nil {
					log.Printf("json unmarshal MsgFileSt.Bytes error:%v, msg.Bytes:%s", err, m.Bytes)
					continue
				}
				if c.fsiReceiver != nil {
					go c.fsiReceiver.Receive(&fsi)
					if fsi.FileState == chain.FILE_STATE_ACTIVE {
						c.stopFsiPoll = true
					}
				}
			}

		case MsgFile:
			if recvFile == nil {
				recvFile, err = os.OpenFile(filepath.Join(c.dir, m.FileName), os.O_RDWR|os.O_TRUNC, os.ModePerm)
				if err != nil {
					log.Println(err)
					c.conn.SendMsg(NewNotifyMsg("", Status_Err))
					time.Sleep(TCP_Message_Interval)
					c.conn.SendMsg(NewCloseMsg("", Status_Err))
					time.Sleep(TCP_Message_Interval)
					return err
				}
			}
			_, err = recvFile.Write(m.Bytes[:m.FileSize])
			if err != nil {
				log.Println(err)
				c.conn.SendMsg(NewNotifyMsg("", Status_Err))
				time.Sleep(TCP_Message_Interval)
				c.conn.SendMsg(NewCloseMsg("", Status_Err))
				time.Sleep(TCP_Message_Interval)
				return err
			}
			switch cap(m.Bytes) {
			case TCP_ReadBuffer:
				readBufPool.Put(m.Bytes)
			default:
			}
		case MsgEnd:
			switch cap(m.Bytes) {
			case TCP_ReadBuffer:
				readBufPool.Put(m.Bytes)
			default:
			}
			info, err := recvFile.Stat()
			if err != nil {
				log.Println(err)
				c.conn.SendMsg(NewNotifyMsg("", Status_Err))
				time.Sleep(TCP_Message_Interval)
				c.conn.SendMsg(NewCloseMsg("", Status_Err))
				time.Sleep(TCP_Message_Interval)
				return err
			}
			if info.Size() != int64(m.FileSize) {
				err = fmt.Errorf("file.size %v rece size %v \n", info.Size(), m.FileSize)
				c.conn.SendMsg(NewNotifyMsg("", Status_Err))
				time.Sleep(TCP_Message_Interval)
				c.conn.SendMsg(NewCloseMsg("", Status_Err))
				time.Sleep(TCP_Message_Interval)
				return err
			}
			recvFile.Close()
			recvFile = nil

		case MsgNotify:
			c.waitNotify <- m.Bytes[0] == byte(Status_Ok)
			switch cap(m.Bytes) {
			case TCP_ReadBuffer:
				readBufPool.Put(m.Bytes)
			default:
			}

		case MsgClose:
			switch cap(m.Bytes) {
			case TCP_ReadBuffer:
				readBufPool.Put(m.Bytes)
			default:
			}
			return errors.New("Close message")

		default:
			switch cap(m.Bytes) {
			case TCP_ReadBuffer:
				readBufPool.Put(m.Bytes)
			default:
			}
			return errors.New("Invalid msgType")
		}
	}

	return err
}

func NewClient(conn *TcpCon, dir string, files []string) *ConMgr {
	return &ConMgr{
		conn:       conn,
		dir:        dir,
		sendFiles:  files,
		waitNotify: make(chan bool, 1),
		stop:       make(chan struct{}),
	}
}

func (c *ConMgr) SetFsiReceiver(fsiReceiver FileStoreInfoReceiver) {
	c.fsiReceiver = fsiReceiver
}

func (c *ConMgr) SendFile(fid string, fsize int64, pkey, signmsg, sign []byte) error {
	c.conn.HandlerLoop()
	go func() {
		_ = c.handler()
	}()

	err := c.sendFile(fid, fsize, pkey, signmsg, sign)
	return err
}

func (c *ConMgr) RecvFile(fid string, fsize int64, pkey, signmsg, sign []byte) error {
	c.conn.HandlerLoop()
	go func() {
		_ = c.handler()
	}()
	err := c.recvFile(fid, fsize, pkey, signmsg, sign)
	return err
}

func (c *ConMgr) sendFile(fid string, fsize int64, pkey, signmsg, sign []byte) error {
	defer func() {
		c.conn.Close()
	}()

	var err error
	var lastmatrk bool

	n := len(c.sendFiles)
	for i := 0; i < n; i++ {
		if (i + 1) == n {
			lastmatrk = true
		}
		err = c.sendSingleFile(c.sendFiles[i], fid, fsize, lastmatrk, pkey, signmsg, sign)
		if err != nil {
			return err
		}
	}
	if c.fsiReceiver != nil {
		//polling file storage state
		for i := 0; !c.stopFsiPoll && i < 100; i++ {
			c.sendFileSt(fid)
			time.Sleep(time.Second * 2)
		}
	}
	c.conn.SendMsg(NewCloseMsg(c.fileName, Status_Ok))
	time.Sleep(time.Second)
	return err
}

func (c *ConMgr) recvFile(fid string, fsize int64, pkey, signmsg, sign []byte) error {
	defer func() {
		c.conn.Close()
	}()

	//log.Println("Ready to recvhead: ", fid)
	c.conn.SendMsg(NewRecvHeadMsg(fid, pkey, signmsg, sign))
	timerHead := time.NewTimer(time.Second * 5)
	defer timerHead.Stop()
	select {
	case ok := <-c.waitNotify:
		if !ok {
			return fmt.Errorf("receive server failed WaitNotify Msg")
		}
	case <-timerHead.C:
		return fmt.Errorf("wait server msg timeout")
	}

	_, err := os.Create(filepath.Join(c.dir, fid))
	if err != nil {
		c.conn.SendMsg(NewCloseMsg(fid, Status_Err))
		return err
	}
	//log.Println("Ready to recvfile: ", fid)
	c.conn.SendMsg(NewRecvFileMsg(fid))

	waitTime := fsize / 1024 / 10
	if waitTime < 5 {
		waitTime = 5
	}

	timerFile := time.NewTimer(time.Second * time.Duration(waitTime))
	defer timerFile.Stop()
	select {
	case ok := <-c.waitNotify:
		if !ok {
			return fmt.Errorf("send err")
		}
	case <-timerFile.C:
		return fmt.Errorf("wait server msg timeout")
	}
	c.conn.SendMsg(NewCloseMsg(fid, Status_Ok))
	//time.NewTimer(time.Second * 3)
	return nil
}

func (c *ConMgr) rpcStyleSendMsg(fn func() error, timeoutBySeconds int, msg string) error {
	if err := fn(); err != nil {
		return err
	}
	log.Printf("%s has sent, wait for server response...", msg)
	timer := time.NewTimer(time.Duration(timeoutBySeconds) * time.Second)
	defer timer.Stop()
	select {
	case ok := <-c.waitNotify:
		if !ok {
			return errors.Errorf("receive server WaitNotify error for msg:%s", msg)
		}
	case <-timer.C:
		return errors.Errorf("wait server response timeout:%d seconds for msg:%s", timeoutBySeconds, msg)
	}
	return nil
}

func (c *ConMgr) sendSingleFile(filePath string, fid string, fsize int64, lastmark bool, pkey, signmsg, sign []byte) error {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("open file err %v \n", err)
		return err
	}
	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()

	log.Println("🙌 Ready to send file:", filePath)
	fileInfo, _ := file.Stat()
	c.rpcStyleSendMsg(func() error {
		c.conn.SendMsg(NewHeadMsg(fileInfo.Name(), fid, lastmark, pkey, signmsg, sign))
		return nil
	}, 10, "HeadMsg")

	readBuf := sendBufPool.Get().([]byte)
	defer func() {
		sendBufPool.Put(readBuf)
	}()
	log.Println("head msg completed, begin transfer file content...")
	for !c.conn.IsClose() {
		n, err := file.Read(readBuf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		c.conn.SendMsg(NewFileMsg(c.fileName, n, readBuf[:n]))
	}

	log.Println("file content transfered, begin send end msg")
	waitTime := fileInfo.Size() / 1024 / 10
	if waitTime < 10 {
		waitTime = 10
	}
	c.rpcStyleSendMsg(func() error {
		c.conn.SendMsg(NewEndMsg(c.fileName, fid, uint64(fileInfo.Size()), uint64(fsize), lastmark))
		return nil
	}, int(waitTime), "EndMsg")

	return nil
}

func (c *ConMgr) sendFileSt(fid string) error {
	return c.rpcStyleSendMsg(func() error {
		c.conn.SendMsg(NewFileStMsg(fid))
		return nil
	}, 3, "FileStMsg")
}
