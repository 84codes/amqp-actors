require 'bunny'

module AmqpActors
  class AmqpQueues
    class << self
      attr_accessor :connections
      attr_reader :amqp_url, :client

      def configure(cfg)
        @amqp_url = cfg[:amqp_url]
        @client = cfg[:client]
        self
      end
    end

    # Instance methods
    def initialize(&blk)
      instance_eval(&blk) if block_given?
      self.class.connections ||= {}
      self.class.client ||= Bunny
      raise ArgumentError, 'Consutructor requires a AMQP connection url' if amqp_url.nil?
      @bunny = AmqpQueues.connections[amqp_url] ||= AmqpQueues.client.new(amqp_url)
    end

    def amqp_url(url = nil)
      if url
        @amqp_url = url
      else
        @amqp_url ||= self.class.amqp_url
      end
    end

    def queue_name(q = nil)
      if q
        @queue_name = q
      else
        @queue_name
      end
    end

    def routing_keys(rks = nil)
      if rks
        @routing_keys = rks
      else
        @routing_keys ||= []
      end
    end

    def exchange(x = nil)
      if x
        @exchange = x
      else
        @exchange
      end
    end

    def start_actor(type)
      @bunny.start
      cfg = {
        exchange: exchange,
        routing_keys: routing_keys,
        amqp_url: amqp_url,
        queue_name: queue_name,
      }
      type.inbox = Channel.new(@bunny, type, cfg)
      type.running = true
    end

    def stop
      AmqpQueues.connections.delete(amqp_url)&.stop
    end
  end

  class Channel
    TOPIC = 'actor.message'.freeze

    def initialize(conn, type, cfg = {})
      @conn = conn
      @prefetch = type.thread_count
      @type = type
      @qname = "AmqpActor::#{cfg[:queue_name] || snake_case(type.to_s)}"
      @exchange = cfg[:exchange] || 'amq.topic'
      @routing_keys = (cfg[:routing_keys] || []) << TOPIC

      @pub_chan = create_pub_channel
      @pub_exchange = @pub_chan.topic(@exchange, durable: true)
      subscribe
    end

    def closed?
      @conn.closed?
    end

    def push(msg)
      push_to(TOPIC, msg)
    end

    def push_to(rk, msg)
      publish(rk, msg)
    end

    def size
      @q&.message_count
    end

    def close
      @pub_chan&.close
      @sub_chan&.close
    end

    private

    def create_pub_channel
      ch = @conn.create_channel
      ch.confirm_select
      ch
    end

    def create_sub_channel
      ch = @conn.create_channel(nil, @prefetch)
      ch.prefetch @prefetch * 2
      ch
    end

    def subscribe
      @sub_chan = create_sub_channel
      x = @sub_chan.topic(@exchange, durable: true)
      @q = @sub_chan.queue @qname, durable: true
      @routing_keys.each { |rk| @q.bind(x, routing_key: rk) }
      @q.subscribe(manual_ack: true, block: false, exclusive: true) do |delivery, _headers, body|
        begin
          msg = Marshal.load(body)
          @type.new.push msg
          @sub_chan.acknowledge(delivery.delivery_tag, false)
        rescue => e
          print "[ERROR] \n #{e.backtrace.join("\n ")}\n"
          sleep 1
          @sub_chan.reject(delivery.delivery_tag, true)
        end
      end
    rescue Bunny::AccessRefused => e
      print "#{e.inspect} retrying in 3\n"
      sleep 3
      @sub_chan&.close
      retry
    end

    def publish(rk, msg)
      @pub_exchange.publish Marshal.dump(msg), {
        routing_key: rk,
        persistent: true,
        content_type: 'text/plain',
      }
      success = @pub_chan.wait_for_confirms
      raise "[ERROR] error=publish reason=not-confirmed" unless success
    rescue Timeout::Error
      print "[WARN] publish to #{topic} timed out, retrying\n"
      retry
    end

    def snake_case(camel_cased_word)
      camel_cased_word.to_s.gsub(/::/, '/')
      .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
      .gsub(/([a-z\d])([A-Z])/, '\1_\2')
      .tr("-", "_")
      .downcase
    end
  end
end
