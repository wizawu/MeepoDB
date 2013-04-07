.PHONY: all clean

dbsrc = meepodb/blocks.go meepodb/cola.go meepodb/config.go \
		meepodb/epoll.go meepodb/extent.go meepodb/gpoll.go \
		meepodb/net.go meepodb/proto.go meepodb/realloc.go \
		meepodb/storage.go

bin: meepodb-cli meepodb-server

all: $(bin)

meepodb-cli: $(dbsrc)
	go build meepodb-cli.go

meepodb-server: $(dbsrc)
	go build meepodb-server.go

clean:
	-rm $(bin)
