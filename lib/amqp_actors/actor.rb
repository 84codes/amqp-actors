require 'set'

module AmqpActors
  module DSL
    module InstanceMethods
      def die
        self.class.die
      end

      def push(msg)
        instance_exec(msg, &self.class.act_block)
      end
    end

    module ClassMethods
      attr_reader :backend_instance, :act_block, :running

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
        raise NotConfigured, 'you must provide an act block' unless @act_block
        @backend_instance&.push(msg) unless @backend_instance&.closed? && @running
      end

      def backend(clazz, &blk)
        raise ArgumentError, "Backend must implement :start and :stop" unless valid_backend? clazz
        @backend = clazz
        @backend_block = blk
        @backend_instance&.stop
        start_backend(@backend) if AmqpActors::System.running?
      end

      def start_backend(default_backend)
        @running = true
        @backend_instance = (@backend || default_backend).new(self, &@backend_block)
        @backend_instance.start
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
        @backend_instance&.size
      end

      def die
        @backend_instance&.stop
        @running = false
      end

      def address
        if @backend_instance.respond_to?(:name)
          @backend_instance.name
        else
          to_s
        end
      end

      def helpers(&block)
        class_eval(&block) if block_given?
      end

      private

      def valid_types?(value)
        return true if @message_type.nil?
        if @message_type.is_a?(Hash) && value.respond_to?(:all?)
          key_value = @message_type.flatten
          key_value.size == 2 && value.is_a?(key_value.first) &&
            value.all? { |t| t.is_a?(key_value.last) }
        else
          a = value.is_a?(@message_type)
          !a.nil? && a
        end
      rescue TypeError
        false
      end

      def valid_backend?(clazz)
        require_methods = %i(start stop)
        require_methods & clazz.instance_methods == require_methods
      end
    end
  end

  class Actor
    extend DSL::ClassMethods
    include DSL::InstanceMethods
  end
end
