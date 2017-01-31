module AmqpActors
  class MemoryQueues
    def initialize
      @threads = {}
    end

    def queue(_type, _thread_count)
      Queue.new
    end

    # @TODO remove dead threads from the array periodically
    def start_actor(type)
      @threads[type] = Array.new(type.thread_count) do
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

    def running_threads(type = nil)
      if type
        @threads[type].select(&:alive?).count
      else
        @threads.values.flatten.select(&:alive?).count
      end
    end
  end
end
