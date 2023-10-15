#!/bin/bash

case "$1" in
    "create")
        go run cmd/create/main.go
    ;;
    "delete")
        go run cmd/delete/main.go
    ;;
    "insert")
        go run cmd/insert/main.go
    ;;
    "update")
        go run cmd/update/main.go
    ;;
    "random")
        go run cmd/random/main.go
    ;;
    "isdag")
        go run cmd/isdag/main.go
    ;;
    "isequal")
        go run cmd/isequal/main.go
    ;;
esac