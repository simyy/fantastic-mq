## Fantastic-MQ

[![Build Status](https://travis-ci.org/fantasticlab/fantastic-mq.svg?branch=master)](https://travis-ci.org/fantasticlab/fantastic-mq)
![JDK](https://img.shields.io/badge/jdk-openjdk8-9cf)
![GitHub](https://img.shields.io/github/license/fantasticlab/fantastic-mq)

`Fantastic-MQ` is a simple MQ framework based on Java.

### Architecture

![Architecture](/architecture.png)

**Broker**

`Broker` is the transfer station of message.

To implement order consume, `Topic` contains multi order `Queue`.

Producer use `key` to select `Queue`, such as

```
{
    "key": "2",
    "topic": "test",
    "body": "hello1"
}
```

**Group**

`Group` is used for isolation of consumers.

The differ from groups in a topic is `Offset`, which is the progress of consumer.

**Offset**

Every group maintain a offset in a `Topic` of `Broker`.

For exception situation, Fantastic-MQ use a dead letter queue(死信队列) to record the offset of pulled.

And, there is a default timeout for the message in dead letter queue.

**Consumer**

For consumer, it must pull the offset of message forwardly from `Broker`.

> Push mode will effect the performance of Broker, it is not recommended.

**Persistence**

Fantastic-MQ provide two model for persistence of message.

The first model is `Memory Storage Model`, which use a CopyOnWriteArrayList for persistence.

The second model is `File Storage Model`, which use file system for persistence.

The message frame in Persistence use `JSON`, and store messages in a file named `db.data`, such as

```
{"key":null,"topic":"topic1","body":"body1"}{"key":null,"topic":"topic2","body":"body2"}
```

To find the position of data, it need a `db.index` to avoid the traverse of data.

The latest offset of data and index in `File Storage Model` is stored in a file named `db.offset`.


> For performance, Fantastic-MQ implemented Sequential Write.

### TODO

* `sendfile`
* `replica`


### License

```
MIT License

Copyright (c) 2019 Fantastic Lab

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```