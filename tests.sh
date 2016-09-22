#!/bin/bash
set -e
set -x
go test -cpu 2 -v -race $(find . -type d -not -path '*vendor*' -not -path '*.git*' )
go run -race example/loopback/main.go
go run -race example/hurtmeplenty/main.go -streams=2048
