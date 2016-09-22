#!/bin/bash
set -e
set -x
go test -cpu 2 -v -race -bench $(find . -type d -not -path '*vendor*' -not -path '*.git*' )
go run -race example/loopback/main.go
go run -race example/hurtmeplenty/main.go -streams=4096
go test -bench $(find . -type d -not -path '*vendor*' -not -path '*.git*' )
