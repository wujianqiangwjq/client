package main

import (
	// "fmt"
	"time"

	"github.com/wujianqiangwjq/huobi"
	"github.com/wujianqiangwjq/mongo"
)

type Pair struct {
	topic    string
	listener func(data *huobi.JSON)
	backend  *huobi.HuoBi
}

func (pair *Pair) Sub() {
	pair.backend.Subcribe(pair.topic, pair.listener)
}

var Collection *mongo.MongoCollection

func init() {
	db := mongo.Client.GetDb("bits")
	Collection = db.GetCollection("btc_min")

}

func main() {
	go Collection.HandleLoop()
	timeout := time.Duration(20 * time.Second)
	client, err := huobi.DefaultConnect(timeout)
	if err != nil {
		panic(err)
		return
	}

	kline := Pair{
		topic:   "market.btcusdt.kline.1min",
		backend: client,
	}

	kline.listener = func(data *huobi.JSON) {
		tick := data.Get("tick")
		resdata := make(map[string]interface{})
		resdata["_id"] = tick.Get("id").MustInt64()
		resdata["data"] = map[string]interface{}{
			"amount": tick.Get("amount").MustFloat64(),
			"close":  tick.Get("close").MustFloat64(),
			"count":  tick.Get("count").MustInt(),
			"vol":    tick.Get("vol").MustFloat64(),
			"open":   tick.Get("open").MustFloat64(),
			"high":   tick.Get("high").MustFloat64(),
			"low":    tick.Get("low").MustFloat64(),
		}
		resdata["push_key"] = "data"
		//data format: {"_id":1,"data":{"_id":1,"open":12.7..}, push_key:"data"}
		go func(data map[string]interface{}) {
			//fmt.Println(data)
			Collection.Push(data)
		}(resdata)

	}

	kline.Sub()
	defer kline.backend.Close()

	kline.backend.KeepAlived()

}
