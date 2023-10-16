package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	fruits := [][]byte{
		[]byte("Apple"),
		[]byte("Banana"),
		[]byte("Cherry"),
		[]byte("Date"),
		[]byte("Elderberry"),
		[]byte("Fig"),
		[]byte("Grape"),
		[]byte("Honeydew"),
		[]byte("Icaco"),
		[]byte("Jackfruit"),
	}

	start1 := time.Now()

	var wg1 sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg1.Add(1)
		go func() {
			defer wg1.Done()
			var buffer1 []byte
			for i := 0; i < 10_000_000; i++ { 
				for _, fruit := range fruits {				
					buffer1 = append(buffer1, fruit...)				
				}
			}
			fmt.Println(len(buffer1))
		}()
	}
	wg1.Wait()

	elapsed1 := time.Since(start1)

	fmt.Println("Append fruit: ", elapsed1)

	start2 := time.Now()

	var wg2 sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			var buffer2 []byte
			for i := 0; i < 10_000_000; i++ { 
				for _, fruit := range fruits {
					if string(fruit) == "Apple" {
						buffer2 = append(buffer2, fruit...)
					}
				}
			}
			fmt.Println(len(buffer2))
		}()
	}
	wg2.Wait()

	elapsed2 := time.Since(start2)

	fmt.Println("Append fruit when apple exist: ", elapsed2)
}
