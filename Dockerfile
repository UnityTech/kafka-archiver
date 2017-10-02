FROM golang

ARG go_package
RUN mkdir -p $GOPATH/src/${go_package}/
ADD . $GOPATH/src/${go_package}/
WORKDIR $GOPATH/src/${go_package}

RUN go get -d -v ./...
RUN go install
