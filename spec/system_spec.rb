describe AmqpActors::System do
  it 'should start/stop MemoryQueues' do
    class StartMemoryActor < AmqpActors::TestActor; end
    AmqpActors::System.start
    AmqpActors::System.running?.must_equal(true)
    AmqpActors::System.stop
    AmqpActors::System.running?.must_equal(false)
  end

  it 'should start/stop AmqpQueues' do
    AmqpActors::System.start(
      default_backend: AmqpActors::AmqpQueues.configure(client: BunnyMock, amqp_url: '')
    )
    class StartAmqpActor < AmqpActors::Actor; end
    AmqpActors::System.running?.must_equal(true)
    AmqpActors::System.stop
    AmqpActors::System.running?.must_equal(false)
  end
end
