require_relative 'spec_helper'

describe AmqpActors::AmqpQueues do
  before do
    AmqpActors::AmqpQueues.configure(client: BunnyMock, amqp_url: '')
    AmqpActors::System.start(default_backend: AmqpActors::AmqpQueues)
  end

  after do
    AmqpActors::System.stop
  end

  it 'should push messages' do
    class AmqpActor < AmqpActors::TestActor
      backend AmqpActors::AmqpQueues do
        amqp_url 'amqp://localhost/test'
        amqp_pub_url 'amqp://localhost/test'
        amqp_sub_url 'amqp://localhost/test'
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
    expect(AmqpActor.output).must_equal(expected)
  end

  describe :content_handler do
    it 'should handle :serialize' do
      class SerializeActor < AmqpActors::TestActor
        backend AmqpActors::AmqpQueues do
          content_type :serialize
        end

        act do |msg|
          output msg
        end
      end

      expected = OpenStruct.new(a: 'a')
      SerializeActor.push(expected)
      expect(SerializeActor.output).must_equal(expected)
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
      expect(JsonActor.output).must_equal(expected)
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
      expect(PlainActor.output).must_equal(expected)
    end
  end

  it 'should raise if not configured' do
    class NotConfiguredActor < AmqpActors::Actor
      backend AmqpActors::AmqpQueues
    end
    expect(proc { NotConfiguredActor.push(1) }).must_raise(AmqpActors::NotConfigured)
  end

  it 'should pust_to rks' do
    class PushToActor < AmqpActors::TestActor
      act do |msg|
        output msg
      end
    end
    AmqpActors::AmqpQueues.push_to(PushToActor.address, 1)
    PushToActor.output
  end
end
