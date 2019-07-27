package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//


	//用于排序后按序写入新文件
	var keys []string

	//具有相同key的value归类到一起
	var kvPairs = make(map[string][]string)


	//此处nMap指的是由nMap个不同的mapTask处理后的中间文件(原先就不是同一个文件)
	for i := 0; i < nMap; i++ {

		//打开存储中间数据的文件
		intermediateFile, err := os.Open(reduceName(jobName, i, reduceTask))
		if err != nil {
			log.Fatal(err)
		}

		var kv KeyValue

		//先将中间数据解码
		fileDecoder := json.NewDecoder(intermediateFile)
		err = fileDecoder.Decode(&kv)
		//只要不出错，循环地取出kv对，直到不存在为止
		for err == nil {
			_, exist := kvPairs[kv.Key]
			if !exist {
				//keys数组不存在这个key的话则加入，用于排序
				keys = append(keys, kv.Key)
			}
			kvPairs[kv.Key] = append(kvPairs[kv.Key], kv.Value)
			err = fileDecoder.Decode(&kv)
		}
	}

	sort.Strings(keys)

	out, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	outFileEncoder := json.NewEncoder(out)


	//将kvPairs中的value聚合成一个value，再和原先的key组合，写入最终的结果文件
	for _, key := range keys {
		value := reduceF(key, kvPairs[key])
		kv := KeyValue{key, value}
		if err = outFileEncoder.Encode(kv); err != nil {
			log.Fatal(err)
		}
	}

}
