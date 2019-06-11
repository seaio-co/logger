package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

type elasticLogger struct {
	Addr     string `json:"addr"`
	Index    string `json:"index"`
	Level    string `json:"level"`
	LogLevel int
	Open     bool
	Es       *elasticsearch.Client
	Mu       sync.RWMutex
}

// Init 初始化
func (e *elasticLogger) Init(jsonConfig string) error {
	if len(jsonConfig) == 0 {
		return nil
	}

	err := json.Unmarshal([]byte(jsonConfig), &e)
	if err != nil {
		return err
	}

	if e.Open == false {
		return nil
	}

	if lv, ok := LevelMap[e.Level]; ok {
		e.LogLevel = lv
	}
	err = e.connectElastic()
	if err != nil {
		return err
	}
	return nil
}

// LogWrite 写操作
func (e *elasticLogger) LogWrite(when time.Time, msgText interface{}, level int) error {

	if level > e.LogLevel {
		return nil
	}

	msg, ok := msgText.(string)
	if !ok {
		return nil
	}

	if e.Es == nil {
		err := e.connectElastic()
		if err != nil {
			return err
		}
	}

	go func() {

		now := time.Now().UnixNano()
		dateTime := strconv.FormatInt(now, 10)
		req := esapi.IndexRequest{
			Index:      e.Index,
			DocumentID: dateTime,
			Body:       strings.NewReader(msg),
			Refresh:    "true",
		}
		res, _ := req.Do(context.Background(), e.Es)
		res.Body.Close()
	}()

	return nil
}

// Destroy 销毁
func (e *elasticLogger) Destroy() {
	e.Es = nil
}

// connectElastic 链接elasticsearch
func (e *elasticLogger) connectElastic() (err error) {
	cfg := elasticsearch.Config{Addresses: []string{e.Addr}}
	e.Es, err = elasticsearch.NewClient(cfg)
	if err != nil {
		return errors.New(fmt.Sprintf("Get elastic client error %v", err))
	}
	return nil
}

func init() {
	Register(AdapterElastic, &elasticLogger{LogLevel: LevelTrace})
}
