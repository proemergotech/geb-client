FROM golang:1.13

ENV PROJECT_NAME=geb-client
ENV ROOT_PACKAGE=github.com/proemergotech/$PROJECT_NAME

ADD . $GOPATH/src/$ROOT_PACKAGE
WORKDIR $GOPATH/src/$ROOT_PACKAGE

RUN chmod +x test/entrypoint.sh

ENTRYPOINT ["test/entrypoint.sh"]

CMD ["go", "test", "--race", "-v", "./test/..."]
