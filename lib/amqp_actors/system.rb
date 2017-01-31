require 'set'
require 'amqp_actors/backend/memory'

module AmqpActors
  module System
    @actors = Set.new
    @running = false
    @backend = AmqpActors::MemoryQueues # @TODO use amqp as default

    def self.start
      @running = true
      @actors.each { |a| start_actor(a) }
    end

    def self.stop
      @running = false
    end

    def self.running?
      @running
    end

    # @TODO these should be private to the module
    def self.add(actor)
      @actors.add(actor)
      @backend.start_actor(actor) if @running
    end

    def self.push(msg, type)
      @actors.select { |a| a.is_a?(type) }.each { |a| a.push(msg) }
    end

    class << self
      attr_accessor :backend
    end
  end
end
