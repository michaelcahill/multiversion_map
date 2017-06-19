all: test_mvmap

test_mvmap: test_mvmap.cpp mvmap.h
	g++ -std=c++14 -ggdb -I/usr/local/include -Wall -o $@ $<
