require_relative 'config'
AmqpActors::System.start

describe AmqpActors do
  it 'should push messages' do
    class MessageActor < AmqpActors::TestActor
      act do |msg|
        output msg
      end
    end

    expected = 'test'
    MessageActor.push(expected)
    MessageActor.output.must_equal(expected)
  end

  it 'should die' do
    class DieActor < AmqpActors::TestActor
      act do |msg|
        output msg
        die
      end
    end

    DieActor.push(1)
    DieActor.output
    AmqpActors::System.running_threads(DieActor).must_equal(0)
  end

  it 'should raise for wrong message type' do
    class TypedTestActor < AmqpActors::TestActor
      message_type String
      act {}
    end

    proc { TypedTestActor.push(1) }.must_raise(ArgumentError)
  end
end
