#!/bin/bash

case "$1" in
    "observable")
        go run observable/main.go
    ;;

    "observer")
        go run observer/*.go
esac