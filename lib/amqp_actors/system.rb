require 'set'
require 'amqp_actors/backend/memory'
require 'amqp_actors/backend/amqp'

module AmqpActors
  module System
    @actors = Set.new
    @running = false
    @backends = {}

    def self.start
      @running = true
      @actors.each { |a| start_actor(a) }
    end

    def self.stop
      @running = false
      @backends.each { |_, b| b.stop }
    end

    def self.running?
      @running
    end

    class << self
      attr_accessor :amqp_url
    end

    def self.configure(&blk)
      instance_eval(&blk)
    end

    # @TODO these should be private to the module
    def self.backend(b)
      case b
      when :amqp
        raise NotConfigured, 'Configure amqp url before using the :amqp backend' unless @amqp_url
        AmqpQueues.new(Bunny.new(@amqp_url))
      when :amqp_mock
        AmqpQueues.new(BunnyMock.new)
      else
        AmqpActors::MemoryQueues.new # @TODO use amqp as default
      end
    end

    def self.running_threads(actor)
      @backends[actor].running_threads(actor)
    end

    def self.add(actor)
      @actors.add(actor)
      start_actor(actor) if @running
    end

    def self.start_actor(actor)
        b = backend(actor.selected_backend)
        @backends[actor] = b
        b.start_actor(actor)
    end

    def self.push(msg, type)
      @actors.select { |a| a.is_a?(type) }.each { |a| a.push(msg) }
    end

    class NotConfigured < StandardError; end
  end
end
