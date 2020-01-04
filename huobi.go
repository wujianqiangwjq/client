package main

import (
	"fmt"

	"github.com/wujianqiangwjq/huobi"
	"github.com/wujianqiangwjq/mongo"
)

type Pair struct {
	topic    string
	listener func(data *huobi.JSON)
}
type Result struct {
	Id     int64   `json:"_id"`
	Amount float64 `json: "amount"`
	Close  float64 `json: "close"`
	Count  int     `json: "count"`
	Vol    float64 `json: "vol"`
}

func (r *Result) ToMap() map[string]interface{} {
	res := make(map[string]interface{})
	res["_id"] = r.Id
	res["amount"] = r.Amount
	res["close"] = r.Close
	res["count"] = r.Count
	res["vol"] = r.Vol
	return res
}

var Res Result
var Collection *mongo.MongoCollection

func init() {
	Res = Result{Id: int64(0)}
	db := mongo.Client.GetDb("bits")
	Collection = db.GetCollection("btc_min")

}

func main() {
	client, err := huobi.DefaultConnect()
	if err != nil {
		panic(err)
		return
	}

	kline := Pair{
		topic: "market.btcusdt.kline.1min",
	}

	kline.listener = func(data *huobi.JSON) {
		tick := data.Get("tick")
		sid := tick.Get("id").MustInt64()
		samount := tick.Get("amount").MustFloat64()
		sclose := tick.Get("close").MustFloat64()
		scount := tick.Get("count").MustInt()
		svol := tick.Get("vol").MustFloat64()
		fmt.Println("new:", sid, "old:", Res.Id)
		if Res.Id == 0 {
			fmt.Println("old:0")
			Res.Id = sid
			Res.Amount = samount
			Res.Close = sclose
			Res.Count = scount
			Res.Vol = svol
		} else {
			if sid != Res.Id {
				fmt.Println("old!=new")
				resdata := Res.ToMap()
				fmt.Println(resdata)
				go func(data map[string]interface{}) {
					Collection.Create(data)
				}(resdata)

			}
			Res.Id = sid
			Res.Amount = samount
			Res.Close = sclose
			Res.Count = scount
			Res.Vol = svol

		}

	}
	client.Subcribe(kline.topic, kline.listener)
	defer client.Close()

	client.Loop()

}
