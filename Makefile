GOFMT=gofmt
GC=go build

VERSION := $(shell git describe --always --tags --long)
BUILD_NODE_PAR = -ldflags "-X main.Version=$(VERSION)" #

ARCH=$(shell uname -m)
SRC_FILES = $(shell git ls-files | grep -e .go$ | grep -v _test.go)

cmp: $(SRC_FILES)
	$(GC)  $(BUILD_NODE_PAR) -o cmp cmd/main.go

cmp-cross: cmp-windows cmp-linux cmp-darwin

cmp-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GC) $(BUILD_NODE_PAR) -o cmp-windows-amd64.exe main.go

cmp-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GC) $(BUILD_NODE_PAR) -o cmp-linux-amd64 main.go

cmp-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GC) $(BUILD_NODE_PAR) -o cmp-darwin-amd64 main.go

tools-cross: tools-windows tools-linux tools-darwin

format:
	$(GOFMT) -w cmd/main.go

clean:
	rm -rf *.8 *.o *.out *.6 *exe
	rm -rf cmp cmp-*
