module AmqpActors
  module DSL
    module InstanceMethods
      def die
        self.class.die
      end

      # @TODO these methods should not be part of DSL
      def push(msg)
        instance_exec(msg, &self.class.act_block)
      end
    end

    module ClassMethods
      attr_accessor :inbox, :act_block, :thread_count, :running

      def inherited(subclass)
        subclass.inbox = System.backend.new
        System.add(subclass)
      end

      def act(&block)
        raise ArgumentError, 'act must recieve a block' unless block_given?
        @act_block = block
      end

      def push(msg)
        unless valid_types?(msg)
          raise ArgumentError, "Illegal message type, expected #{@message_type}"
        end
        @inbox.push msg unless @inbox.closed?
      end

      def message_type(type)
        @message_type = type
      end

      def thread_count(count = 1)
        @thread_count = count
      end

      def inbox_size
        @inbox.size
      end

      def die
        @inbox.close
        @running = false
      end

      def helpers(&block)
        class_eval(&block) if block_given?
      end

      # @TODO these methods should not be part of DSL
      def valid_types?(type)
        if type.is_a?(Enumerable)
          key_value = @message_type.flatten
          key_value.size == 2 && type.is_a?(key_value.first) &&
            type.all? { |t| t.is_a?(key_value.last) }
        else
          @message_type.nil? || type.is_a?(@message_type)
        end
      end
    end
  end

  class Actor
    extend DSL::ClassMethods
    include DSL::InstanceMethods
  end
end
