package main

import (
	"fmt"
	"goBeanstalk"
)

func main() {
	var conn, err = goBeanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		fmt.Println("no connect to beanstalk server...")
		return
	}
	fmt.Printf("hello, beanstalk...\n")
	ch := make(chan int)
	// worker
	go func() {
		fmt.Printf("Hello, dispatch...\n")
		worker := goBeanstalk.NewWorker(conn, "testtube", 4)
		worker.Dispatch()

		ch <- 1
	}()

	<-ch
}
