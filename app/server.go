package main

import (
	"fmt"
	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	c, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	defer func(c net.Conn) {
		err = c.Close()
		if err != nil {
			os.Exit(1)
		}
	}(c)

	for i := 0; i < 2; i++ {
		_, err = c.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("err replying: ", err.Error())
			os.Exit(1)
		}
	}
}
