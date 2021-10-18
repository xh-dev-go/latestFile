package main

import (
	"encoding/json"
	"flag"
	"github.com/segmentio/kafka-go"
	"github.com/xh-dev-go/xhUtils/xhKafka/header"
	"github.com/xh-dev-go/xhUtils/xhKafka/producer"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

const VERSION = "1.0.0"

type UpdateType string

const (
	LATEST UpdateType = "LATEST"
)

type UpdateMsg struct {
	FilePath string
	Type     UpdateType
}

func (updateMsg *UpdateMsg) ToString() string {
	if bytes, err := json.Marshal(updateMsg); err != nil {
		panic(err)
	} else {
		return string(bytes)
	}
}

var last fs.FileInfo = nil

func detect(
	file string, update chan UpdateMsg, delay int64) {

	var firstTime = false
	for {
		var tempLast fs.FileInfo = nil
		if files, err := ioutil.ReadDir(file); err != nil {
			panic(err)
		} else {
			for _, file := range files {
				if !file.IsDir() {
					if tempLast == nil {
						tempLast = file
					} else if tempLast.ModTime().Before(file.ModTime()) {
						tempLast = file
					}
				}
			}

			updateLast := func() {
				if !firstTime {
					firstTime = true
				} else {
					update <- UpdateMsg{
						Type:     LATEST,
						FilePath: filepath.Join(file, last.Name()),
					}
				}
			}
			if last == nil && tempLast != nil {
				last = tempLast
				updateLast()
			} else if last != nil && tempLast != nil && last.Name() != tempLast.Name() {
				last = tempLast
				updateLast()
			}
		}

		if delay > 0 {
			duration := time.Duration(delay) * time.Millisecond
			time.Sleep(duration)
		}
	}
}

func main() {
	const cmdVersion = "version"
	const cmdPath = "directory"
	const cmdDelay = "delay"
	const cmdKafkaHost, cmdKafkaTopic = "kafka-host", "kafka-topic"
	var directory string
	var delay int64
	var checkVersion bool
	var kafkaHost, kafkaTopic string

	flag.StringVar(&kafkaHost, cmdKafkaHost, "", "kafka host")
	flag.StringVar(&kafkaTopic, cmdKafkaTopic, "", "kafka topic")
	flag.BoolVar(&checkVersion, cmdVersion, false, "check the version fo application")
	flag.StringVar(&directory, cmdPath, "", "the path to detect")
	flag.Int64Var(&delay, cmdDelay, -1, "the time delay for check directory stat")
	flag.Parse()

	if checkVersion {
		println(VERSION)
		os.Exit(0)
	}

	if directory == "" {
		print("Please input directory path")
		flag.Usage()
		os.Exit(1)
	}

	if fInfo, err := os.Stat(directory); err != nil && os.IsNotExist(err) {
		println("File not exists")
		flag.Usage()
		os.Exit(1)
	} else if err != nil {
		panic(err)
	} else if !fInfo.IsDir() {
		println("File is not directory")
		flag.Usage()
		os.Exit(1)
	}

	var hasKafka = false
	var xhProducer producer.XhKafkaProducer
	if kafkaHost != "" && kafkaTopic != "" {
		hasKafka = true
		xhProducer = producer.New(kafkaHost)
	}

	if delay == -1 {
		delay = 1000
	}

	fileUpdate := make(chan UpdateMsg)

	go detect(directory, fileUpdate, delay)

	for {
		select {
		case msg := <-fileUpdate:
			if hasKafka {
				var header = header.KafkaHeaders{}
				header = header.Add("Type",string(LATEST))
				header = header.Add("File", msg.FilePath)
				err := xhProducer.SimpleSend(
					kafka.Message{
						Value: []byte(""),
					},
				)
				if err != nil {
					println(err.Error())
				}
			}
			println(msg.ToString())
		}
	}
}
