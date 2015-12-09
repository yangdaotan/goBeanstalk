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
	chc := make(chan int)
	// consumer
	go func() {
		fmt.Printf("hello, consumer...\n")
		groupName := "testtube0"
		_, err := conn.Watch(groupName)
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

	<-chc
}
