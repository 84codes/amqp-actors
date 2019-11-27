require_relative 'spec_helper'

describe AmqpActors::System do
  it 'should start/stop MemoryQueues' do
    class StartMemoryActor < AmqpActors::TestActor; end
    AmqpActors::System.start
    expect(AmqpActors::System.running?).must_equal(true)
    AmqpActors::System.stop
    expect(AmqpActors::System.running?).must_equal(false)
  end

  it 'should start/stop AmqpQueues' do
    AmqpActors::System.start(
      default_backend: AmqpActors::AmqpQueues.configure(client: BunnyMock, amqp_url: '')
    )
    class StartAmqpActor < AmqpActors::Actor; end
    expect(AmqpActors::System.running?).must_equal(true)
    AmqpActors::System.stop
    expect(AmqpActors::System.running?).must_equal(false)
  end
end
