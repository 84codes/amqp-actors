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

      def push_to(rks, msg, cfg = {})
        raise NotConfigured, "Can't .push_to without configured amqp_[pub_]url" unless @pub_url
        unless @publisher
          conn = @connections[@pub_url] ||= AmqpQueues.client.new(@pub_url)
          @publisher = Publisher.new(nil, conn, cfg)
        end
        @publisher.publish(rks, msg)
      end
    end

    # Instance methods
    def initialize(type, &blk)
      instance_eval(&blk) if block_given?
      @type = type
      @pub_url ||= self.class.pub_url
      @sub_url ||= self.class.sub_url
      @content_type = :serialize
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

    def queue_name(queue_name)
      @queue_name = queue_name
    end

    def routing_keys(*routing_keys)
      @routing_keys = routing_keys
    end

    def exchange(exchange)
      @exchange = exchange
    end

    def content_type(content_type)
      @content_type = content_type
    end

    def start
      @pub_conn.start
      @sub_conn.start
      cfg = {
        exchange: @exchange,
        routing_keys: @routing_keys,
        queue_name: @queue_name,
        content_type: @content_type
      }
      @inbox = Channel.new(@pub_conn, @sub_conn, @type, cfg)
    end

    def stop
      AmqpQueues.connections.delete(@pub_url)&.stop
      AmqpQueues.connections.delete(@sub_url)&.stop
    end
  end

  class Channel
    def initialize(pub_conn, sub_conn, type, cfg = {})
      @publisher = Publisher.new(type, pub_conn, cfg)
      @subscriber = Subscriber.new(type, sub_conn, cfg)
      @subscriber.subscribe
    end

    def closed?
      @publisher.closed? || @subscriber.closed?
    end

    def close
      @publisher.close
      @subscriber.close
    end

    def size
      @subscriber.size
    end

    def push(msg)
      @publisher.publish(@subscriber.to_s, msg)
    end

    def push_to(rk, msg)
      @publisher.publish(rk, msg)
    end
  end

  module ContentHandler
    def self.content_handler(content_type)
      case content_type
      when :serialize
        [Serialize, 'text/plain']
      when :Json
        [Json, 'application/json']
      else
        [Plain, 'text/plain']
      end
    end

    class Plain
      def self.encode(data)
        data
      end

      def self.decode(data)
        data
      end
    end

    class Json
      def self.encode(data)
        data.to_json
      end

      def self.decode(data)
        JSON.parse(data, symbolize_keys: true)
      end
    end

    class Serialize
      def self.encode(data)
        Marshal.dump(data)
      end

      def self.decode(data)
        Marshal.load(data)
      end
    end
  end

  class Publisher
    def initialize(_type, conn, cfg)
      @conn = conn
      @chan = create_pub_channel
      @exchange = @chan.topic(cfg[:exchange] || 'amq.topic', durable: true)
      @encoder, @content_type = ContentHandler.content_handler(cfg[:content_type])
    end

    def create_pub_channel
      ch = @conn.create_channel
      ch.confirm_select
      ch
    end

    def publish(rks, msg)
      if rks.is_a?(Array)
        rks.each { |rk| publish(rk, msg) }
        return
      end
      @exchange.publish @encoder.encode(msg), {
        routing_key: rks,
        persistent: true,
        content_type: @content_type,
      }
      success = @chan.wait_for_confirms
      raise "[ERROR] error=publish reason=not-confirmed" unless success
    rescue Timeout::Error
      print "[WARN] publish to #{rks} timed out, retrying\n"
      retry
    end

    def close
      @chan.close
    end

    def closed?
      @conn.closed?
    end
  end

  class Subscriber
    def initialize(type, conn, cfg)
      @prefetch = type.thread_count
      @conn = conn
      @type = type
      @qname = "AmqpActor::#{cfg[:queue_name] || snake_case(type.to_s)}"
      @exchange_type = cfg[:exchange] || 'amq.topic'
      @routing_keys = (cfg[:routing_keys] || []) << @qname
      @encoder, @content_type = ContentHandler.content_handler(cfg[:content_type])
    end

    def create_sub_channel
      ch = @conn.create_channel(nil, @prefetch)
      ch.prefetch @prefetch * 2
      ch
    end

    def subscribe
      @chan = create_sub_channel
      x = @chan.topic(@exchange_type, durable: true)
      @q = @chan.queue @qname, durable: true
      @routing_keys.each { |rk| @q.bind(x, routing_key: rk) }
      @q.subscribe(manual_ack: true, block: false, exclusive: true) do |delivery, _headers, body|
        begin
          msg = @encoder.decode(body)
          @type.new.push msg
          @chan.acknowledge(delivery.delivery_tag, false)
        rescue => e
          print "[ERROR] #{e.message}\n #{e.backtrace.join("\n ")}\n"
          sleep 1
          @chan.reject(delivery.delivery_tag, true)
        end
      end
    rescue Bunny::AccessRefused => e
      print "#{e.inspect} retrying in 3\n"
      sleep 3
      @chan&.close
      retry
    end

    def close
      @chan.close
    end

    def closed?
      @conn.closed?
    end

    def to_s
      @qname
    end

    def size
      @q&.message_count
    end

    private

    def snake_case(camel_cased_word)
      camel_cased_word.to_s.gsub(/::/, '/')
        .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
        .gsub(/([a-z\d])([A-Z])/, '\1_\2')
        .tr("-", "_")
        .downcase
    end
  end
end
