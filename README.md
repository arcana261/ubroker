[![Build Status](https://travis-ci.org/maedeazad/ubroker.svg?branch=master)](https://travis-ci.org/maedeazad/ubroker) [![Join the chat at https://gitter.im/maedeazad-ubroker/community](https://badges.gitter.im/maedeazad-ubroker/community.svg)](https://gitter.im/maedeazad-ubroker/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# ubroker

Welcome to phase2!

# Current Phase 2

Greetings! As you may already know, In this phase of the project we are about to take
our broker system which we implemented prevously, take out it's HTTP‌ (JSON) layer and replace
it with a neat gRPC‌ mechanism !

Before we begin any further, I'd like to point out that our master branch has changed and
contains implementation made by your friend, and ofcourse, my dear colleague at Cafebazaar,
[Milad Norouzi](https://github.com/miladosos) as reference implementation. I've made slight
alterations to his implementations to make it compatible with our new protocol buffer specification.

So.. list of changes I've made to master branch are as follows:

* `api/ubroker.proto`: This file now contains proto specification for RPC's we'd like to have.
* `Makefile`: I've added a dependency for tests (a library which generates random free ports) and ofcourse, directives to generate GO‌ code from `api/ubroker.proto` into `pkg/ubroker/ubroker.pb.go`. The first time you try to execute `make check` or `make ubroker` this autogenerated file will be generated from proto file.
* `pkg/ubroker/ubroker.go`: I've modified our `Broker` interface to use structs defined in our protobuffer `api/ubroker.proto`.
* `internal/broker/core.go`: Contains implementation from my colleague [Milad Norouzi](https://github.com/miladosos)
* `internal/broker/core_test.go`: I've made modifications to make it complient with structures defined in our protobuf file `api/ubroker.proto`
* `internal/server/http.go`: I've made modifications to make it complient with structures defined in our protobuf file `api/ubroker.proto`
* `internal/server/http_test.go`: I've made modifications to make it complient with structures defined in our protobuf file `api/ubroker.proto`
* `internal/server/grpc.go`: This new file is the file you shoud implement (which implements `BrokerServer` autogenerated from `api/ubroker.proto` into `pkg/ubroker/ubroker.pb.go`)
* `internal/server/grpc_test.go`: This new file contains tests that I'd like to PASS :) and you should too!
* `cmd/ubroker/main.go`: I've made modifications to start gRPC‌ server from implementation in `internal/server/grpc.go`.

Your task is as follows:
1. Make sure you have `protoc` installed. (the protobuf compiler)
2. Run `make dev-dependencies` to download all dependencies and ofcourse, autogenerate `pkg/ubroker/ubroker.pb.go`
3. Implement file at `internal/server/grpc.go`
4. Submit your pull request!

# Previous Phase 1

Happy new year to you, students!

We wanted to set the stage for continuous series of exercises to develop
a message broker.

A message broker is a system that acts as a hub to distribute messages in a
microservice environment and is an important communication pattern. There are
various benefits of using messaging systems, which the most important are:

1. **Async processing**: process tasks asynchronously
2. **Fault tolerance**: sometimes processing of some tasks can be paused without causing major downtimes to system by queueing them and processing them later
3. **Load balancing**: distribute work among some workers, if processing takes time or there is heavy load, we can queue them and process them later

And much more! I highly encourage you two watch [this](https://www.youtube.com/watch?v=rXi5CLjIQ9k)
awesome video! I also encourage you to look at RabbitMQ design.

So, the messaging system is expected to do following operation:

1. Provide a `/publish` HTTP API that let's clients publish messages to a queue. Or to say enqueue messages in our queue.
2. Provide a `/fetch` HTTP‌ API that let's clients fetch messages from queue.
3. Provide a `/acknowledge/{id}` HTTP‌‌ API that clients call after their processing is finished so that we can remove item from queue. This is important because we can have a `at least once` guarantee on our queue: If a client crashes after receiving a message from queue, we can return them automatically to queue after a timeout. This way we can ensure no message is removed from queue without clients acknowledging they have successfully and gracefully processed them. This is why this system is called to have an `at least once` guarantee because clients might see messages **at least once**. By stating that our gaurantee is at-least once, we are not referring to a randomized behavior that we might re-send a message to clients, but rather clients might see messages more than once because of failures or errors in their systems (like database transaction failure, etc.) And we will ensure that we keep supplying enqueued message until clients confirm that they have successfully processed fetched message.
4. Provide a `/requeue/{id}` HTTP‌ API that let's clients to requeue a message. This is useful when clients run into error in their system and want to retry a message or let some other worker handle them.

Now... Don't you guys worry! We have already laid out a boilerplate code beautifully so that you can learn how to code in GO and also validate your results! The steps are as follows:

1. Fork this project in Github
2. Clone forked project into `GOPATH`

```bash
cd $GOPATH/github.com/<your username>
git clone <clone URL>
cd ubroker

make dev-dependencies
```

3. Open `internal/broker/core.go` file. This file lays out an implementation of  `pkg.ubroker.Broker` interface located in `pkg/ubroker/ubroker.go` file. You should read the GoDoc for the interface. We have accompanies the code with tests that let's you on a TDD‌(Test Driven Development) approach of implementing this interface. You can read tests in `internal/broker/core_test.go` file to know what is missing and what needs to be implemented.

4. Run tests via `make check`. If all pass proceed to step 5. We require your code to be threadsafe thus we run race detector tests against your code.

5. Setup `Travis CI` for project. You just need to login to travis, we have supplied travis CI test file so you should not worry. You can search the internet how to do so.

6. Submit a `Pull Request` in github. We evaluate submissions that pass the tests **ONLY**.

## Questions?

Feel free to open an issue or chat on gitter as provided by gitter badge above!
