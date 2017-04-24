require 'bunny'
require 'json'
require 'zlib'

module AmqpActors
  # rubocop:disable Style/TrivialAccessors
  class AmqpQueues
    class << self
      attr_accessor :connections
      attr_reader :pub_url, :sub_url, :client

      def configure(cfg)
        @pub_url = cfg[:amqp_pub_url] || cfg[:amqp_url]
        @sub_url = cfg[:amqp_sub_url] || cfg[:amqp_url]
        @client = cfg[:client] || Bunny
        @connections ||= {}
        @content_type = cfg[:content_type]
        self
      end

      def push_to(rks, msg, cfg = {})
        raise NotConfigured, "Can't .push_to without configured amqp_[pub_]url" unless @pub_url
        unless @publisher
          conn = @connections[@pub_url] ||= AmqpQueues.client.new(@pub_url)
          conn.start
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

    def content_encoding(encoding)
      @content_encoding = encoding
    end

    def closed?
      @inbox.closed?
    end

    def push(msg)
      @inbox.push_to(@inbox.name, msg)
    end

    def name
      @inbox.name
    end

    def start
      @pub_conn.start
      @sub_conn.start
      cfg = {
        exchange: @exchange,
        routing_keys: @routing_keys,
        queue_name: @queue_name,
        content_type: @content_type,
        content_encoding: @content_encoding
      }
      @inbox = Channel.new(@pub_conn, @sub_conn, @type, cfg)
      self
    end

    def stop
      @inbox&.close
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

    def name
      @subscriber.to_s
    end

    def push_to(rk, msg)
      @publisher.publish(rk, msg)
    end
  end

  class Publisher
    def initialize(_type, conn, cfg)
      @conn = conn
      @chan = create_pub_channel
      @cfg = cfg
      @exchange = @chan.topic(cfg[:exchange] || 'amq.topic', durable: true)
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
      content_handler = ContentHandler.resolve_content_handler(msg, @cfg[:content_type])
      @exchange.publish encode(content_handler.encode(msg)), {
        routing_key: rks,
        persistent: true,
        content_type: content_handler.encoding,
        content_encoding: @content_encoding
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

    private

    def encode(body)
      case @cfg[:content_encoding].to_s
      when 'gzip'
        inflater = Zlib::Inflate.new
        inflater.sync(body)
        inflater.inflate(body)
      else
        body
      end
    end
  end

  class Subscriber
    def initialize(type, conn, cfg)
      @prefetch = type.thread_count
      @conn = conn
      @type = type
      @qname = "AmqpActor::#{cfg[:queue_name] || snake_case(type.to_s)}"
      @exchange_type = cfg[:exchange] || 'amq.topic'
      @cfg = cfg
    end

    def create_sub_channel
      ch = @conn.create_channel(nil, @prefetch)
      ch.prefetch @prefetch * 2
      ch
    end

    def subscribe(qname = nil)
      @chan ||= create_sub_channel
      qname ||= @qname
      x = @chan.topic(@exchange_type, durable: true)
      @q = @chan.queue qname, durable: true
      routing_keys = (@cfg[:routing_keys] || []) << qname
      routing_keys.each { |rk| @q.bind(x, routing_key: rk) }
      @q.subscribe(manual_ack: true, block: false, exclusive: true) do |delivery, headers, body|
        begin
          data = parse(body, headers)
          if block_given?
            Timeout.timeout AmqpActors::System.timeout do
              yield delivery, headers, data
            end
          else
            @type.new.push(parse(body, headers))
          end
          @chan.acknowledge(delivery.delivery_tag, false)
        rescue => e
          print "[ERROR] #{e.inspect} #{e.message}\n #{e.backtrace.join("\n ")}\n"
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

    def decode(content_encoding, body)
      case content_encoding.to_s
      when 'gzip'
        StringIO.open(body) do |io|
          gz = Zlib::GzipReader.new(io)
          begin
            return gz.read
          ensure
            gz.close
          end
        end
      else
        body
      end
    end

    def parse(body, headers)
      raw_data = decode(headers.content_encoding, body)
      content_handler = ContentHandler
        .resolve_content_handler(raw_data, @cfg[:content_type] || headers.content_type)
      content_handler.decode(raw_data)
    end
  end

  module ContentHandler
    @@content_types = []

    def self.register(content_type)
      @@content_types << content_type
    end

    def self.content_handler(content_type_sym)
      content_type = @@content_types.find { |ct| ct.name.end_with? content_type_sym.to_s }
      raise NotConfigured, "Unknown content_type=#{content_type}. register?" unless content_type
      content_type
    end

    def self.resolve_content_handler(msg, content_type)
      if content_type.is_a?(Symbol)
        content_handler(content_type)
      elsif content_type.is_a?(String)
        @@content_types.find { |ct| ct.encoding == content_type }
      else
        @@content_types.find { |ct| ct.can_handle?(msg) }
      end
    end

    class ContentType
      class << self
        def can_handle?(data)
          decode(encode(data)) && true
        rescue
          false
        end

        def name
          to_s.downcase
        end
      end
    end

    class Plain < ContentType
      def self.encoding
        'text/plain'
      end

      def self.encode(data)
        data
      end

      def self.decode(data)
        data
      end
    end

    class Json < ContentType
      def self.encoding
        'application/json'
      end

      def self.encode(data)
        data.to_json
      end

      def self.decode(data)
        JSON.parse(data, symbolize_names: true)
      end
    end

    class Serialize < ContentType
      def self.encoding
        'text/plain'
      end

      def self.encode(data)
        Marshal.dump(data)
      end

      def self.decode(data)
        Marshal.load(data)
      end
    end

    register(Json)
    register(Serialize)
    register(Plain)
  end
end
