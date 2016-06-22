package skykiss

import (
	"testing"
)

func BenchmarkStructChan(b *testing.B) {
	ch := make(chan struct{})
	go func() {
		for {
			<-ch
		}
	}()
	for i := 0; i < b.N; i++ {
		ch <- struct{}{}
	}
}

func BenchmarkBoolChan(b *testing.B) {
	ch := make(chan bool)
	go func() {
		for {
			<-ch
		}
	}()
	for i := 0; i < b.N; i++ {
		ch <- true
	}
}

func BenchmarkIntChan(b *testing.B) {
	ch := make(chan int)
	go func() {
		for {
			<-ch
		}
	}()
	for i := 0; i < b.N; i++ {
		ch <- 1
	}
}
