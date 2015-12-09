package main

import (
	"fmt"
	"goBeanstalk"
	"time"
)

func main() {
	var conn, err = goBeanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		fmt.Println("no connect to beanstalk server...")
		return
	}

	fmt.Printf("hello, beanstalk...\n")
	chp := make(chan int)
	// productor
	go func() {
		fmt.Printf("hello, productor...\n")
		err := conn.Use("testtube")
		id, err := conn.Put([]byte("hello"), 1, 0, 30*time.Second)
		if err != nil {
			panic(err)
		}
		fmt.Println("job", id)
		chp <- 1
	}()
	<-chp
}
