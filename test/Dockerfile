FROM golang:1.10

ENV PROJECT_NAME=geb-client-go
ENV ROOT_PACKAGE=gitlab.com/proemergotech/$PROJECT_NAME
ENV DEP_VERSION=0.5.0

RUN wget -O /usr/local/bin/dep https://github.com/golang/dep/releases/download/v$DEP_VERSION/dep-linux-amd64 && chmod +x /usr/local/bin/dep

ADD . $GOPATH/src/$ROOT_PACKAGE

WORKDIR $GOPATH/src/$ROOT_PACKAGE

RUN dep ensure -vendor-only

RUN chmod +x test/entrypoint.sh

ENTRYPOINT ["test/entrypoint.sh"]

CMD ["go", "test", "--race", "-v", "./test/..."]
