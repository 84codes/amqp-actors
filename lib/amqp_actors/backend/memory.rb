module AmqpActors
  class MemoryQueues
    def initialize(type, &_blk)
      @threads = []
      @type = type
    end

    # @TODO remove dead threads from the array periodically
    def start
      @threads = Array.new(@type.thread_count) do
        Thread.new do
          loop do
            break unless System.running? && @type.running
            begin
              msg = @type.inbox.pop
              @type.new.push(msg)
            rescue => e
              print "[ERROR] #{e.message} \n #{e.backtrace.join("\n ")}\n"
            end
          end
        end
      end
      Queue.new
    end

    def stop
      # noop
    end

    def running_threads
      @threads.select(&:alive?).count
    end
  end
end
