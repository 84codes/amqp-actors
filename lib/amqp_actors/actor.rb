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
      attr_accessor :inbox, :act_block, :running
      attr_reader :backend_instance

      def inherited(subclass)
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
        @inbox&.push msg unless @inbox&.closed?
      end

      # @TODO these should be private to the module
      def backend(clazz, &blk)
        raise ArgumentError, "Must implement :start_actor and :stop" unless valid_backend? clazz
        @backend = clazz
        @backend_block = blk
      end

      def start_backend(default_backend)
        @backend_instance = (@backend || default_backend).new(&@backend_block)
        @backend_instance.start_actor(self)
      end

      def running_threads
        @backend_instance.running_threads(self)
      end

      def message_type(type)
        @message_type = type
      end

      def thread_count(count = nil)
        if count
          @thread_count = count
        else
          @thread_count ||= 1
        end
      end

      def inbox_size
        @inbox&.size
      end

      def die
        @inbox&.close
        @running = false
      end

      def helpers(&block)
        class_eval(&block) if block_given?
      end

      # @TODO these methods should not be part of DSL
      def valid_types?(type)
        return true if @message_type.nil?
        if type.is_a?(Enumerable)
          key_value = @message_type.flatten
          key_value.size == 2 && type.is_a?(key_value.first) &&
            type.all? { |t| t.is_a?(key_value.last) }
        else
          type.is_a?(@message_type)
        end
      end

      def valid_backend?(clazz)
        %i(start_actor stop) & clazz.instance_methods
      end

      class NotConfigured < AmqpActors::Error; end
    end
  end

  class Actor
    extend DSL::ClassMethods
    include DSL::InstanceMethods
  end
end
