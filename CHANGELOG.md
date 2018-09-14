# Release Notes

# 0.6.0 / 2018-09-14
- removed support for multiple struct tags in codec (only 1 can be used now)
- tag is now mandatory (no longer uses struct field name as fallback)
- replaced json library

# 0.5.2 / 2018-04-10
- fixed publish deadlock introduced by 0.5.1
- improved concurrency handling (tested with --race flag)

# 0.5.1 / 2018-04-03
- error handling fixes
- inline documentation
- improved performance with low MaxGoroutines setting

# 0.5.0 / 2018-03-28
- during OnEvent, deliver messages on a new goroutines until "MaxGoroutines" limit reached, the default value is 1
- add "MaxGoroutines" option to OnEvent

# 0.4.1 / 2018-02-19
- added option to set publish context

# 0.4.0 / 2018-02-16
- added fluent interface for Publish & OnEvent
- added context handling
- added middleware support

# 0.3.0 / 2018-02-13
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
- fixed concurrency issues

## 0.0.1 / 2017-08-07
- project created
