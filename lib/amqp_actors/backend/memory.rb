module AmqpActors
  class MemoryQueues
    @@threads = {}

    # @TODO remove dead threads from the array periodically
    def self.start_actor(type)
      @@threads[type] = Array.new(type.thread_count) do
        type.running = true
        Thread.new do
          loop do
            break unless System.running? && type.running
            begin
              msg = type.inbox.pop
              type.new.push(msg)
            rescue => e
              print "[ERROR] #{e.inspect}\n"
              print e.backtrace
            end
          end
        end
      end
    end

    def self.running_threads(type = nil)
      if type
        @@threads[type].select(&:alive?).count
      else
        @@threads.values.flatten.select(&:alive?).count
      end
    end

    def initialize
      @queue = Queue.new
    end

    def method_missing(method_sym, *arguments, &block)
      if @queue.respond_to?(method_sym)
        @queue.send(method_sym, *arguments, &block)
      else
        super
      end
    end

    def respond_to_missing?(method_sym, include_private = false)
      @queue.respond_to?(method_sym, include_private)
    end
  end
end
