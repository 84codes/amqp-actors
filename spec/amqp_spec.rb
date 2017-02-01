require_relative 'config'
require 'bunny-mock'

describe AmqpActors::AmqpQueues do
  before do
    AmqpActors::System.start
  end

  after do
    AmqpActors::System.stop
  end

  it 'should push messages' do
    class AmqpActor < AmqpActors::TestActor
      backend :amqp_mock # do
        #queue_name 'test' #default "#{actor.class}::actor"
        #routing_key 'test.#' #default queue_name
        #exchange 'amq.topic' #default amq.default
      #end

      act do |msg|
        output msg
      end
    end

    expected = 'test'
    AmqpActor.push(expected)
    AmqpActor.output.must_equal(expected)
  end
end
