require 'bunny'

module AmqpActors
  class AmqpQueues
    def initialize(bunny)
      raise ArgumentError, 'Consutructor requires bunny instance' if bunny.nil?
      @bunny = bunny
    end

    def start_actor(type)
      @bunny.start
      type.inbox = Channel.new(@bunny, type)
      type.running = true
    end

    def stop
      @bunny.stop
    end
  end

  class Channel
    TOPIC = 'actor.message'.freeze

    def initialize(bunny, type)
      @bunny = bunny
      @qname = "amqp_actor.#{snake_case(type.to_s)}"
      @prefetch = type.nr_of_threads
      @pub_chan = create_channel
      @pub_topic = @pub_chan.topic 'amq.topic', durable: true
      @type = type
      subscribe
    end

    def closed?
      @bunny.closed?
    end

    def push(msg)
      publish(msg)
    end

    def size
      @q&.message_count
    end

    def close
      @pub_chan&.close
      @sub_chan&.close
      @bunny.close
    end

    private

    def create_channel
      ch = @bunny.create_channel(nil, @prefetch)
      ch.prefetch @prefetch
      ch.confirm_select
      ch
    end

    def subscribe
      @sub_chan = create_channel
      t = @sub_chan.topic 'amq.topic', durable: true
      @q = @sub_chan.queue @qname, durable: true
      @q.bind(t, routing_key: @qname)
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

    def publish(msg)
      @pub_topic.publish Marshal.dump(msg), {
        routing_key: @qname,
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
