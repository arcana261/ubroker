[![Build Status](https://travis-ci.org/arcana261/ubroker.svg?branch=master)](https://travis-ci.org/arcana261/ubroker) [![Join the chat at https://gitter.im/arcana261-ubroker/community](https://badges.gitter.im/arcana261-ubroker/community.svg)](https://gitter.im/arcana261-ubroker/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# ubroker

Happy new year to you, students!

We wanted to set the stage for continuous series of excersices to develop
a message broker.

A message broker is a system that acts as a hub to distribute messages in a
microservice environment and is an important communication pattern. There are
various benefits of using messaging systems, which the most important are:

1. **Async processing**: process tasks asynchronously
2. **Fault tolerance**: sometimes processing of some tasks can be paused without causing major downtimes to system by queueing them and processing them later
3. **Load balancing**: distribute work among some workers, if processing takes time or there is heavy load, we can queue them and process them later

And much more! I highly encourage you two watch [this](https://www.youtube.com/watch?v=rXi5CLjIQ9k)
awesome video! I also encourage you to look at RabbitMQ design.

So, the messaging system is expected to do following opration:

1. Provide a `/publish` HTTP API that let's clients publish messages to a queue. Or to say enqueue messages in our queue.
2. Provide a `/fetch` HTTP‌ API that let's clients fetch messages from queue.
3. Provide a `/acknowledge/{id}` HTTP‌‌ API that clients call after their processing is finished so that we can remove item from queue. This is important because we can have a `at least once` gurantee on our queue: If a client crashes after receiving a message from queue, we can return them automatically to queue after a timeout. This way we can ensure no message is removed from queue without clients acknowledging they have successfuly and gracefully processed them. This is why this system is called to have a `at least once` gurantee because clients might see messages **at least once**. By stating that our gurantee is at-least once, we are not referring to a randomized behaviour that we might re-send a message to clients, but rather clients might see messages more than once becuase of failures or errors in their systems (like database transaction failure, etc.) And we will ensure that we keep supplying enqueued message until clients confirm that they have successfuly processed fetched message.
4. Provde a `/requeue/{id}` HTTP‌ API that let's clients to requeue a message. This is useful when clients run into error in their system and want to retry a message or let some other worker handle them.

Now... Don't you guys worry! We have already laid out a boilerplate code beautifully so that you can learn how to code in GO and also validate your results! The steps are as follows:

1. Fork this project in Github
2. Clone forked project into `GOPATH`

```bash
cd $GOPATH/github.com/<your username>
git clone <clone URL>
cd ubroker

make dependencies
```

3. Open `internal/broker/core.go` file. This file lays out an implementation of  `pkg.ubroker.Broker` interface located in `pkg/ubroker/ubroker.go` file. You should read the GoDoc for the interface. We have accompanies the code with tests that let's you on a TDD‌(Test Driven Development) approach of implementing this interface. You can read tests in `internal/broker/core_test.go` file to know what is missing and what needs to be implemented.

4. Run tests via `make check`. If all pass proceed to step 5. We require your code to be threadsafe thus we run race detector tests against your code.

5. Setup `Travis CI` for project. You just need to login to travis, we have supplied travis CI test file so you should not worry. You can search the internet how to do so.

6. Submit a `Pull Request` in github. We evaluate submissions that pass the tests **ONLY**.

## Questions?

Feel free to open an issue or chat on gitter as provided by gitter badge above!
