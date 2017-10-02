VERSION := $(shell cat main.go | grep -v '//' | grep -m 1 Version | sed -e 's/Version =//g' | xargs)
name := $(shell basename "$(CURDIR)")
package := $(CURDIR:$(GOPATH)/src/%=%)
registry := quay.io/unity3d

# -- START GENERIC BUILD/PUSH -- #

build:
	docker build \
		--build-arg go_package=$(package) \
		-t \
		$(registry)/$(name):${VERSION} .
	
	docker run \
		--entrypoint "bash" \
		--rm \
		$(registry)/$(name):${VERSION} \
		-c \
		"make test"

push:
	docker push \
		$(registry)/$(name):${VERSION}


# -- START APPLICATION SPECIFIC -- #

test:
	go get -d -t -v ./... && go test -cover -race ./... -v

pprof-goroutine:
	curl http://localhost:6060/debug/pprof/goroutine?debug=1

pprof-heap:
	curl http://localhost:6060/debug/pprof/heap?debug=1


.PHONY: login build push test