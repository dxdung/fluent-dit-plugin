require "fluent/output"

module Fluent

  require 'aws-sdk'
  require 'logger'
  
  class KinesisOutput < BufferedOutput

    require 'fluent/version'
    if Gem::Version.new(Fluent::VERSION) < Gem::Version.new('0.14.12')
      require 'fluent/process'
      include Fluent::DetachMultiProcessMixin
    end
    include Fluent::SetTimeKeyMixin
    include Fluent::SetTagKeyMixin

    PUT_RECORDS_MAX_COUNT = 500
    PUT_RECORD_MAX_DATA_SIZE = 1024 * 1024

    Fluent::Plugin.register_output('kinesis',self)

    config_set_default :include_time_key, true
    config_set_default :include_tag_key,  true

    config_param :aws_key_id,  :string, default: nil, :secret => true
    config_param :aws_sec_key, :string, default: nil, :secret => true
    # The 'region' parameter is optional because
    # it may be set as an environment variable.
    config_param :region,      :string, default: nil
    config_param :ensure_stream_connection, :bool, default: true

    config_param :stream_name,            :string
    config_param :partition_key,          :string, default: nil
    config_param :order_events,           :bool,   default: false

    config_param :debug, :bool, default: false

    def configure(conf)
      log.warn("Deprecated warning: out_kinesis is no longer supported after v1.0.0. Please check out_kinesis_streams out.")
      super
      validate_params

      if @detach_process or (@num_threads > 1)
        @parallel_mode = true
        if @detach_process
          @use_detach_multi_process_mixin = true
        end
      else
        @parallel_mode = false
      end

      @order_events = false
      @dump_class = JSON
    end

    def start
        super
        load_client
        if @ensure_stream_connection
          check_connection_to_stream
        end
    end

    def format(tag, time, record)
      data = {
        data: @dump_class.dump(record),
        partition_key: get_key(:partition_key,record)
      }

      data.to_msgpack
    end

    def write(chunk)
      data_list = chunk.to_enum(:msgpack_each).map{|record|
        build_data_to_put(record)
      }.find_all{|record|
        unless record_exceeds_max_size?(record[:data])
          true
        else
          log.error sprintf('Record exceeds the %.3f KB(s) per-record size limit and will not be delivered: %s', PUT_RECORD_MAX_DATA_SIZE / 1024.0, record[:data])
          false
        end
      }
    end

    private
    def validate_params
      unless @partition_key
        raise Fluent::ConfigError, "'partition_key' is required"
      end
    end

    def load_client
      options = {}
      if @region
        options[:region] = @region
      end

      if @aws_key_id && @aws_sec_key
        options.update(
          access_key_id: @aws_key_id,
          secret_access_key: @aws_sec_key
        )
      end

      if @debug
        options.update(
          logger: Logger.new(log.out),
          log_level: :debug
        )
        # XXX: Add the following options, if necessary
        # :http_wire_trace => true
      end

      @client = Aws::Kinesis::Client.new(options)

    end

    def check_connection_to_stream
      @client.describe_stream(stream_name: @stream_name)
    end

    def get_key(name, record)
      if @random_partition_key
        SecureRandom.uuid
      else
        key = instance_variable_get("@#{name}")
        key_proc = instance_variable_get("@#{name}_proc")

        value = key ? record[key] : record

        if key_proc
          value = key_proc.call(value)
        end

        value.to_s
      end
    end

    def build_data_to_put(data)
        Hash[data.map{|k, v| [k.to_sym, v] }]
    end

    def record_exceeds_max_size?(record_string)
      return record_string.length > PUT_RECORD_MAX_DATA_SIZE
    end
  end
end