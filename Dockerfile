FROM alpine:latest

RUN apk --no-cache add git curl jq bash grep

RUN entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
