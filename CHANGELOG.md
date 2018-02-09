
# Release Notes

# 0.3.0 / 2018-02-12
- added msgpack encoding
- added headers/body separation to events

# 0.2.0 / 2018-02-07
- create an interface for the queue
- move the original implementation to rabbitmq package and keep the interface in the geb package
- unexport `Timeout` struct field from rabbitmq.Queue, it is set through an Option, so don't need to be exposed

# 0.1.6 / 2018-01-09
- fixed fatal error: concurrent map iteration and map write

# 0.1.5 / 2018-01-09
- fixed a bug introduced in 0.1.4 which resulted in panic when rabbitmq server was unavailable

## 0.1.4 / 2018-01-04
- removed reerror dependency
- changed dependency manager from glide to dep
- removed logging of bound queues (client should log them if needed)

## 0.1.3 / 2017-10-16
- added PublishStruct convenience method

## 0.1.2 / 2017-10-16
- added timeout option and reduced default timeout from 30s to 5s

## 0.1.1 / 2017-08-18
- fixed reconnect
- fixed concurency issues

## 0.0.1 / 2017-08-07
- project created
