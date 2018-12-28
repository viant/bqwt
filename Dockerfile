FROM golang:1.11.1-alpine 

ENV NOTVISIBLE "in users profile"
RUN apk add --no-cache curl py-pip jq make gcc musl-dev git

COPY . /bqwt/
WORKDIR /bqwt/server
RUN go build bqwt.go
CMD ["/bqwt/server/bqwt"]
