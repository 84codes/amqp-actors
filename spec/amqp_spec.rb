require_relative 'spec_helper'

describe AmqpActors::AmqpQueues do
  before do
    AmqpActors::System.start(
      default_backend: AmqpActors::AmqpQueues.configure(client: BunnyMock, amqp_url: '')
    )
  end

  after do
    AmqpActors::System.stop
  end

  it 'should push messages' do
    class AmqpActor < AmqpActors::TestActor
      backend AmqpActors::AmqpQueues do
        amqp_url 'amqp://localhost/test'

        queue_name 'test' # default "#{actor.class}::actor"
        routing_keys 'test.#' # default queue_name
        exchange 'amq.topic' # default amq.default
      end

      act do |msg|
        output msg
      end
    end

    expected = 'test'
    AmqpActor.push(expected)
    AmqpActor.output.must_equal(expected)
  end

  describe :content_handler do
    it 'should handle :serilize' do
      class SerializeActor < AmqpActors::TestActor
        backend AmqpActors::AmqpQueues do
          content_type :serilize
        end

        act do |msg|
          output msg
        end
      end

      expected = OpenStruct.new(a: 'a')
      SerializeActor.push(expected)
      SerializeActor.output.must_equal(expected)
    end

    it 'should handle :json' do
      class JsonActor < AmqpActors::TestActor
        backend AmqpActors::AmqpQueues do
          content_type :json
        end

        act do |msg|
          output msg
        end
      end

      expected = { a: 'a' }
      JsonActor.push(expected)
      JsonActor.output.must_equal(expected)
    end

    it 'should handle :plain' do
      class PlainActor < AmqpActors::TestActor
        backend AmqpActors::AmqpQueues do
          content_type :plain
        end

        act do |msg|
          output msg
        end
      end

      expected = 'expected'
      PlainActor.push(expected)
      PlainActor.output.must_equal(expected)
    end
  end
end
