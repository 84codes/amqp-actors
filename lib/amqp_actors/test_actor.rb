module AmqpActors
  class TestActor
    extend DSL::ClassMethods
    include DSL::InstanceMethods

    def output(obj)
      self.class.output = obj
    end

    class << self
      attr_writer :output

      def output
        sleep 0.01 until @output
        @output
      end
    end
  end
end
