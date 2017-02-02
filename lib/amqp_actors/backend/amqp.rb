require 'bunny'

module AmqpActors
  # rubocop:disable Style/TrivialAccessors
  class AmqpQueues
    class << self
      attr_accessor :connections
      attr_reader :pub_url, :sub_url, :client

      def configure(cfg)
        @pub_url = cfg[:amqp_pub_url] || cfg[:amqp_url]
        @sub_url = cfg[:amqp_sub_url] || cfg[:amqp_url]
        @client = cfg[:client]
        self
      end
    end

    # Instance methods
    def initialize(&blk)
      instance_eval(&blk) if block_given?
      @pub_url ||= self.class.pub_url
      @sub_url ||= self.class.sub_url
      self.class.connections ||= {}
      self.class.client ||= Bunny
      if @pub_url.nil? || @sub_url.nil?
        raise NotConfigured, 'use AmqpQueues.configure or AmqpActor#backend to provide a amqp url'
      end
      @pub_conn = AmqpQueues.connections[@pub_url] ||= AmqpQueues.client.new(@pub_url)
      @sub_conn = AmqpQueues.connections[@sub_url] ||= AmqpQueues.client.new(@sub_url)
    end

    def amqp_url(url)
      @pub_url = @sub_url = url
    end

    def amqp_pub_url(url)
      @pub_url = url
    end

    def amqp_sub_url(url)
      @sub_url = url
    end

    def queue_name(q)
      @queue_name = q
    end

    def routing_keys(*rks)
      @routing_keys = rks
    end

    def exchange(x)
      @exchange = x
    end

    def start_actor(type)
      @pub_conn.start
      @sub_conn.start
      cfg = {
        exchange: @exchange,
        routing_keys: @routing_keys,
        queue_name: @queue_name,
      }
      type.inbox = Channel.new(@pub_conn, @sub_conn, type, cfg)
      type.running = true
    end

    def stop
      AmqpQueues.connections.delete(amqp_url)&.stop
    end
  end

  class Channel
    TOPIC = 'actor.message'.freeze

    def initialize(pub_conn, sub_conn, type, cfg = {})
      @pub_conn = pub_conn
      @sub_conn = sub_conn
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
      @pub_conn.closed? || @sub_conn.closed?
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
      @pub_chan.close
      @sub_chan.close
    end

    private

    def create_pub_channel
      ch = @pub_conn.create_channel
      ch.confirm_select
      ch
    end

    def create_sub_channel
      ch = @sub_conn.create_channel(nil, @prefetch)
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
