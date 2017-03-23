module AmqpActors
  class MemoryQueues
    def initialize(type, &_blk)
      @threads = []
      @type = type
      @inbox = Queue.new
    end

    def start
      @threads = Array.new(@type.thread_count) do
        Thread.new do
          loop do
            break unless System.running? && @type.running
            begin
              msg = @inbox.pop
              @type.new.push(msg) unless msg.nil?
            rescue => e
              print "[ERROR] #{e.message} \n #{e.backtrace.join("\n ")}\n"
            end
          end
        end
      end
      self
    end

    def push(msg)
      @inbox.push(msg)
    end

    def size
      @inbox.size
    end

    def stop
      @inbox.close
      @threads.each(&:kill)
    end

    def closed?
      @inbox.closed?
    end

    def running_threads
      @threads.select(&:alive?).count
    end
  end
end
