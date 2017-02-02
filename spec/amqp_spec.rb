require_relative 'config'

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
      #backend AmqpActors::AmqpQueues do
        #amqp_url 'amqp://localhost/test'
#
        #queue_name 'test' #default "#{actor.class}::actor"
        #routing_keys 'test.#' #default queue_name
        #exchange 'amq.topic' #default amq.default
        #puts 'done'
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
