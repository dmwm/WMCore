#!/usr/bin/make

PREFIX=$(shell pwd)

# Targets
LIB_DIR=$(PREFIX)/lib

.PHONY: all
.PHONY: install

build:
	python setup.py build --build-lib=$(LIB_DIR)

setup:
	/bin/mkdir -p $(LIB_DIR)

install: setup build

all: setup build


clean:
	/bin/rm -rf $(LIB_DIR)/*
