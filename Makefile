
BINARY_DIR=bin
BINARY_LINUX=$(BINARY_DIR)/vftt
BINARY_MACOS=$(BINARY_DIR)/vftt
BINARY_WINDOWS=$(BINARY_DIR)/vftt.exe

.PHONY: help build linux mac win clean

help:
	@echo "usage:make <option>"
	@echo "options and effects:"
	@echo "  help: Show help"
	@echo "  build: Build binary file for Linux"
	@echo "  linux: Build binary file for Linux"
	@echo "  mac: Build binary file for MacOS"
	@echo "  win: Build binary file for Windows"
	@echo "  clean: clean all generated binary files"

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BINARY_LINUX) -v

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BINARY_LINUX) -v

mac:
	CGO_ENABLED=0 GOOS=darwin GOARCH=x86 go build -o $(BINARY_MACOS) -v

win:
	CGO_ENABLED=0 GOOS=windows GOARCH=x86 go build -o $(BINARY_WINDOWS) -v

clean:
	rm -rf $(BINARY_DIR)
