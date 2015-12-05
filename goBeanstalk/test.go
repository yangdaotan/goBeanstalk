package main

import (
	"fmt"
	"goBeanstalk"
	"time"
)

var conn, _ = goBeanstalk.Dial("tcp", "127.0.0.1:11300")

func main() {
	fmt.Printf("hello, beanstalk...\n")
	chp := make(chan int)
	chc := make(chan int)
	// productor
	go func() {
		fmt.Printf("hello, productor...\n")
		id, err := conn.Put([]byte("hello"), 1, 0, 30*time.Second)
		if err != nil {
			panic(err)
		}
		fmt.Println("job", id)
		chp <- 1
	}()
	// consumer
	go func() {
		fmt.Printf("hello, consumer...\n")
		id, body, err := conn.Reserve()
		if err != nil {
			panic(err)
		}
		fmt.Println("job", id)
		fmt.Println(string(body))
		err = conn.Delete(id)
		if err != nil {
			panic(err)
		}
		chc <- 1
	}()

	<-chp
	<-chc
}
