require 'set'

module AmqpActors
  module System
    @actors = Set.new
    @running = false
    @threads = {}
    @backend = :in_memory # @TODO use amqp as default

    def self.start
      @running = true
      @actors.each { |a| start_actor(a) }
    end

    def self.stop
      @running = false
    end

    def self.running_threads(type = nil)
      if type
        @threads[type].select(&:alive?).count
      else
        @threads.values.flatten.select(&:alive?).count
      end
    end

    # @TODO these should be private to the module
    def self.add(actor)
      @actors.add(actor)
      start_actor(actor) if @running
    end

    def self.push(msg, type)
      @actors.select { |a| a.is_a?(type) }.each { |a| a.push(msg) }
    end

    # @TODO remove dead threads from the array periodically
    def self.start_actor(type)
      @threads[type] = Array.new(type.thread_count) do
        type.running = true
        Thread.new do
          loop do
            break unless @running && type.running
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

    class << self
      attr_accessor :backend
    end
  end
end
