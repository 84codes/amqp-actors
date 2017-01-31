module AmqpActors
  class MemoryQueues
    def initialize
      @threads = {}
    end

    # @TODO remove dead threads from the array periodically
    def start_actor(type)
      type.inbox = Queue.new
      type.running = true
      @threads[type] = Array.new(type.nr_of_threads) do
        Thread.new do
          loop do
            break unless System.running? && type.running
            begin
              msg = type.inbox.pop
              type.new.push(msg)
            rescue => e
              print "[ERROR] \n #{e.backtrace.join("\n ")}\n"
            end
          end
        end
      end
    end

    def stop
      # noop
    end

    def running_threads(type = nil)
      if type
        @threads[type].select(&:alive?).count
      else
        @threads.values.flatten.select(&:alive?).count
      end
    end
  end
end
