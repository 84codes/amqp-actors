require 'set'
require 'amqp_actors/backend/memory'
require 'amqp_actors/backend/amqp'

module AmqpActors
  class Error < StandardError; end
  class NotConfigured < Error; end

  module System
    @actors = Set.new
    @running = false
    @timeout = 60

    def self.start(cfg = {})
      @default_backend = cfg[:default_backend] || MemoryQueues
      @timeout = cfg[:act_timeout] || @timeout
      @running = true
      @actors.each { |a| a.start_backend(@default_backend) }
    end

    def self.stop
      @running = false
      @actors.each(&:die)
      @default_backend.stop if @default_backend
      @default_backend = nil
      @actors = Set.new
    end

    def self.running?
      @running
    end

    def self.add(actor)
      @actors.add(actor)
      actor.start_backend(@default_backend) if @running
    end

    def self.push(msg, type)
      @actors.select { |a| a.is_a?(type) }.each { |a| a.push(msg) }
    end

    def self.timeout
      @timeout
    end
  end
end
