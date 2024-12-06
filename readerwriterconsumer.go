// Copyright (c) 2024 ALTAI Consulting, Inc and Aleksey Gershgorin. All rights reserved.
// Use of this source code is governed by MIT license that can be found in the LICENSE file.

// Code below was written for the Medium Article "Implementing Reader Writer Pattern with Golang Channels"
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Res represents the response structure pushed through the reply channels from the consumer of the reader and writer requests
type Res[Req, Resp any] struct {
	Request  Req
	Response Resp
	Err      error
}

// Processor is a function type that takes request Req (reader or writer request), executes it on a shared resource,
// and returns result Res[Req, Resp] of this request execution
type Processor[Req, Resp any] func(Req) Res[Req, Resp]

// ReaderWriterConsumer takes two streams of reader and writer requests represented by channels readReqCh and writeReqCh,
// and two processors readProcessor and writeProcessor implementing shared resource reading and writing logic,
// implements multiple reader/exclusive writer request processing on a shared resource,
// and returns two reply channels for communicating responses to the reader and writer requests back to the request producers
func ReaderWriterConsumer[R, W, RResp, WResp any](
	ctx context.Context,
	readReqCh <-chan R,
	writeReqCh <-chan W,
	readProcessor Processor[R, RResp],
	writeProcessor Processor[W, WResp],
) (<-chan Res[R, RResp], <-chan Res[W, WResp]) {
	readRespCh, writeRespCh := make(chan Res[R, RResp]), make(chan Res[W, WResp])
	go func() {
		type reqType int
		const (
			unknown reqType = iota
			read
			write
		)
		var prevReqType, currReqType reqType
		var wg sync.WaitGroup
		for readReqCh != nil || writeReqCh != nil {
			var reqRead R
			var reqWrite W
			prevReqType = currReqType
			select {
			case req, ok := <-readReqCh:
				if ok {
					currReqType = read
					reqRead = req
				} else {
					currReqType = unknown
					readReqCh = nil
				}
			case req, ok := <-writeReqCh:
				if ok {
					currReqType = write
					reqWrite = req
				} else {
					currReqType = unknown
					writeReqCh = nil
				}
			case <-ctx.Done():
				currReqType = unknown
				readReqCh = nil
				writeReqCh = nil
			}
			if currReqType == unknown {
				continue
			}
			if currReqType != prevReqType || currReqType == write {
				wg.Wait()
			}
			switch currReqType {
			case write:
				wg.Add(1)
				go func(req W, respCh chan<- Res[W, WResp]) {
					defer wg.Done()
					respCh <- writeProcessor(req)
				}(reqWrite, writeRespCh)
			case read:
				wg.Add(1)
				go func(req R, respCh chan<- Res[R, RResp]) {
					defer wg.Done()
					respCh <- readProcessor(req)
				}(reqRead, readRespCh)
			}
		}
		wg.Wait()
		close(readRespCh)
		close(writeRespCh)
	}()
	return readRespCh, writeRespCh
}

func main() {
	type readRequest struct {
		key string
	}
	type writeRequest struct {
		key   string
		value string
	}
	type response struct {
		key   string
		value string
	}
	ctx := context.Background()
	readReqCh, writeReqCh := make(chan readRequest), make(chan writeRequest)
	// consumers of the responses to the reader-writer requests
	dict := map[string]string{}
	readProcessor := func(req readRequest) Res[readRequest, response] {
		fmt.Printf("entering read processor for the key '%s'\n", req.key)
		value, ok := dict[req.key]
		res := Res[readRequest, response]{Request: req, Response: response{key: req.key, value: value}}
		if !ok {
			res.Err = fmt.Errorf("key '%s' not found", req.key)
		}
		time.Sleep(3 * time.Second)
		fmt.Printf("exiting read processor for the key '%s'\n", req.key)
		return res
	}
	writeProcessor := func(req writeRequest) Res[writeRequest, response] {
		fmt.Printf("entering write processor for the key '%s'\n", req.key)
		dict[req.key] = req.value
		time.Sleep(3 * time.Second)
		fmt.Printf("exiting write processor for the key '%s'\n", req.key)
		return Res[writeRequest, response]{Request: req, Response: response(req)}
	}
	readerRespCh, writerRespCh := ReaderWriterConsumer(ctx, readReqCh, writeReqCh, readProcessor, writeProcessor)
	var wg sync.WaitGroup
	wg.Add(2)
	go func(readerRespCh <-chan Res[readRequest, response]) {
		defer wg.Done()
		for res := range readerRespCh {
			if res.Err != nil {
				fmt.Println(res.Err.Error())
			} else {
				fmt.Printf("value '%s' was fetched for the key '%s'\n", res.Response.value, res.Response.key)
			}
		}
	}(readerRespCh)
	go func(writerRespCh <-chan Res[writeRequest, response]) {
		defer wg.Done()
		for res := range writerRespCh {
			fmt.Printf("value for the key '%s' was updated to '%s'\n", res.Response.key, res.Response.value)
		}
	}(writerRespCh)
	// producer of the reader-writer requests
	ratings := map[string]string{
		"cusip0001": "BB",
		"cusip0002": "BB",
		"cusip0003": "BB",
		"cusip0004": "BB",
		"cusip0005": "BB",
	}
	newRatings := map[string]string{
		"cusip0001": "AAA",
		"cusip0002": "AAA",
		"cusip0003": "AAA",
		"cusip0004": "AAA",
		"cusip0005": "AAA",
	}
	var wgProducer sync.WaitGroup
	wgProducer.Add(2)
	go func(readReqCh chan<- readRequest) {
		defer wgProducer.Done()
		for key := range ratings {
			readReqCh <- readRequest{key: key}
		}
		time.Sleep(3 * time.Second)
		for key := range ratings {
			readReqCh <- readRequest{key: key}
		}
		time.Sleep(3 * time.Second)
		for key := range ratings {
			readReqCh <- readRequest{key: key}
		}
	}(readReqCh)
	go func(writeReqCh chan<- writeRequest) {
		defer wgProducer.Done()
		time.Sleep(1 * time.Second)
		for key, value := range ratings {
			writeReqCh <- writeRequest{key: key, value: value}
		}
		time.Sleep(4 * time.Second)
		for key, value := range newRatings {
			writeReqCh <- writeRequest{key: key, value: value}
		}
	}(writeReqCh)
	wgProducer.Wait()
	close(readReqCh)
	close(writeReqCh)
	wg.Wait()
}
