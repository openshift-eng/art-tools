.PHONY: build clean

BINARY := fake-kubelet

build:
	CGO_ENABLED=0 go build -cover -covermode=atomic -o $(BINARY) .

clean:
	rm -f $(BINARY)
