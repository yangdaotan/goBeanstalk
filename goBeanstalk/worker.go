// Worker is a server to transport the job in specific tube to group tubes
// the default of watch tube is default, and the number of group tubes is 4
// the default  group tubes named default0, default1, default2, default3
// the tube and number can be set

package goBeanstalk

import (
	// "container/list"
	"fmt"
	"strconv"
	"time"
)

type Worker struct {
	conn     *Conn    // connection to server
	tubeName string   // tubename
	groupNum int      //number of the group tube
	subGroup []string //the slice of group name
}

func NewWorker(conn *Conn, tubeName string, groupNum int) *Worker {
	if groupNum <= 0 {
		groupNum = 4
	}

	if tubeName == "" {
		tubeName = "default"
	}

	subGroup := make([]string, groupNum)

	for i, _ := range subGroup {
		subGroup[i] = tubeName + strconv.Itoa(i)
	}

	return &Worker{conn: conn, tubeName: tubeName, groupNum: groupNum, subGroup: subGroup}
}

// func (worker *Worker) Config(mux *RouteMux) {
// 	mux.Get("/worker/config", worker.Set)
// }
//
// func (worker *Worker) Set(w http.ResponseWriter, r *http.Request) {
// 	params := r.URL.Query()
//
// 	tubeName := params.Get("tube")
// 	groupNum := params.Get("groupnum")
//
// 	if tubeName != nil {
// 		worker.tubeName = tubeName
// 	}
// 	if groupNum != nil {
// 		worker.groupNum = groupNum
// 	}
//
// 	worker.Dispatch()
// }

// worker
func (worker *Worker) Dispatch() {
	_, err := worker.conn.Watch(worker.tubeName)
	if err != nil {
		panic(err)
	}
	for {
		id, body, err := worker.conn.Reserve()
		fmt.Println(id)
		if err != nil {
			panic(err)
		}

		c := make(chan int)
		go worker.Forword(body, c)
		<-c
		err = worker.conn.Delete(id)
		if err != nil {
			panic(err)
		}
	}
}

func (worker *Worker) Forword(body []byte, c chan int) {
	for _, tube := range worker.subGroup {
		err := worker.conn.Use(tube)
		if err != nil {
			panic(err)
		}

		id, err := worker.conn.Put(body, 1, 0, 30*time.Second)
		if err != nil {
			panic(err)
		}
		fmt.Println("job", id, tube)
	}
	c <- 1
}

func (worker *Worker) GetGroup() []string {
	return worker.subGroup
}
