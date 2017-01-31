require_relative 'config'
require 'bunny-mock'

describe AmqpActors::AmqpQueues do
  before do
    AmqpActors::System.backend = AmqpActors::AmqpQueues.new BunnyMock.new
    AmqpActors::System.start
  end

  after do
    AmqpActors::System.stop
  end

  it 'should push messages' do
    class AmqpActor < AmqpActors::TestActor
      act do |msg|
        output msg
      end
    end

    expected = 'test'
    AmqpActor.push(expected)
    AmqpActor.output.must_equal(expected)
  end
end
