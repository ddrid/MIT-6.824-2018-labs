package main

import (
	"os"
	"strconv"
	"strings"
	"unicode"
)
import "fmt"
import "../mapreduce"

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// Your code here (Part V).
	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}

	//分割文本为单词
	words := strings.FieldsFunc(value, f)

	var kv []mapreduce.KeyValue

	//与文档名组合成kv
	for _, word := range words {
		kv = append(kv, mapreduce.KeyValue{word, document})
	}

	return kv
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	// Your code here (Part V).

	mark := make(map[string]bool)
	result := make([]string, 0)

	for _, value := range values {
		if !mark[value] {
			mark[value] = true
			result = append(result, value)
		}
	}

	numOfDoc := strconv.Itoa(len(result))
	joinedStrings := strings.Join(result, ",")

	return numOfDoc + " " + joinedStrings
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
