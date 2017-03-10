require_relative 'spec_helper'

describe AmqpActors do
  before do
    AmqpActors::System.start
  end

  after do
    AmqpActors::System.stop
  end

  it 'should push messages' do
    class MessageActor < AmqpActors::TestActor
      act do |msg|
        output msg
      end
    end

    expected = 'test\nsad'
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

    DieActor.backend_instance.running_threads.must_equal(0)
  end

  it 'should raise for wrong message type' do
    class TypedTestActor < AmqpActors::TestActor
      message_type String
      act {}
    end

    proc { TypedTestActor.push(1) }.must_raise(ArgumentError)
  end

  it 'A backend must implement :start and :stop' do
    class TestBackend; end

    proc do
      class CustomBackendActor < AmqpActors::TestActor
        backend TestBackend

        act {}
      end
    end.must_raise(ArgumentError)
  end

  it 'should get and set thread_count' do
    class ThreadCountActor < AmqpActors::TestActor
      thread_count 2
      act do |_msg|
        output ThreadCountActor.thread_count
      end
    end

    ThreadCountActor.push('whatever')
    ThreadCountActor.output.must_equal(2)
  end

  it 'should be able to use helpers' do
    class HelpersActor < AmqpActors::TestActor
      helpers do
        def sum(a, b)
          a + b
        end
      end
      act do |msg|
        output sum(1, msg)
      end
    end

    HelpersActor.push(1)
    HelpersActor.output.must_equal(2)
  end

  it 'should handle collections message type' do
    class CollectionsActor < AmqpActors::TestActor
      message_type Array => String
      act do |msg|
        output msg
      end
    end

    CollectionsActor.push(["1"])
    CollectionsActor.output.must_equal(["1"])
    proc { CollectionsActor.push("hej") }.must_raise(ArgumentError)
  end

  it 'should return inbox size' do
    class ImboxSizeActor < AmqpActors::TestActor
      act do |msg|
        output msg
      end
    end

    ImboxSizeActor.inbox_size.must_equal 0
    ImboxSizeActor.push(1)
    ImboxSizeActor.inbox_size.must_equal 1
    ImboxSizeActor.output
    ImboxSizeActor.inbox_size.must_equal 0
  end
end
