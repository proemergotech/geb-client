#FROM golang:1.8-alpine3.6
FROM golang:1.8-jessie

#RUN apk add --update --no-cache \
#		bash \
#		ca-certificates \
#		wget \
#		git \
#	&& update-ca-certificates

RUN \
    cd /tmp \
    && wget https://glide.sh/get \
    && sh ./get \
    && rm ./get

RUN go get github.com/codegangsta/gin

VOLUME /go/bin/gitlab.com/proemergotech/geb-client-go
VOLUME /go/src/gitlab.com/proemergotech/geb-client-go

EXPOSE 80

COPY ./entrypoint.sh /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

#CMD ["go", "run", "/go/src/gitlab.com/proemergotech/geb-client-go/examples.go"]
CMD ["run"]

