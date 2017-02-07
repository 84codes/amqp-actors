[![Build Status](https://travis-ci.org/84codes/amqp-actors.svg?branch=master)](https://travis-ci.org/84codes/amqp-actors)
[![Code Climate](https://codeclimate.com/github/84codes/amqp-actors/badges/gpa.svg)](https://codeclimate.com/github/84codes/amqp-actors)
[![Test Coverage](https://codeclimate.com/github/84codes/amqp-actors/badges/coverage.svg)](https://codeclimate.com/github/84codes/amqp-actors/coverage)
# AmqpActors
Actor system with amqp or in memory queues as backend. Provides an easy to use DSL and a high level abstraction to the AMQP protocol.
## Hello World
```ruby
class MyActor < AmqpActors::Actor
  act do |msg|
    puts msg
  end
end

AmqpActors.Sytem.start
MyActor.push 'hello'
```

## Actor DSL
**Act (required)**. Message processor
```ruby
act do |msg|
  ...
end
```

**Typed messages**. Built in type checking of messages.
```ruby
message_type Numeric
message_type Array => String
message_type Set => Object
```
**Concurrency**. Define number of threads for an actor.
```ruby
thread_count 10
```
**Helpers**. Define helper methods for your actor.
```ruby
helpers do
  def do_stuff
    ...
  end
end
```
**Inbox size**. Check number of enqueued messages.
```ruby
inbox_size # => 0
```
**Destory actor**. Closes inbox and kills all actor threads for current actor.
```ruby
die
```

## Message passing
Messages are sent by using an actors class method `push`

## System module
**Start**. Start the actor system.
```ruby
AmqpActors.System.start
```
**Stop**. Stops the system.
```ruby
AmqpActors.System.stop
```

## Testing
Subclass `AmqpActors::TestActor` in tests. It uses an in memory queue as backed by default and provides an extra method for passing values out of the actor. This can then be read syncronously in the test.
```ruby
class TestActor < AmqpActors::TestActor
  act do |msg|
    output msg + 1
  end
end

..
TestActor.push 0
TestActor.output # => 1
```
