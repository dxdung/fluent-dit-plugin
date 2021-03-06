require "fluent/input"

module Fluent

  require 'active_record'

  class SQLInput < Input
    Plugin.register_input('sql', self)

    # For fluentd v0.12.16 or earlier
    class << self
      unless method_defined?(:desc)
        def desc(description)
        end
      end
    end

    desc 'RDBMS host'
    config_param :host, :string
    desc 'RDBMS port'
    config_param :port, :integer, :default => nil
    desc 'RDBMS driver name.'
    config_param :adapter, :string
    desc 'RDBMS database name'
    config_param :database, :string
    desc 'RDBMS login user name'
    config_param :username, :string, :default => nil
    desc 'RDBMS login password'
    config_param :password, :string, :default => nil, :secret => true
    desc 'RDBMS socket path'
    config_param :socket, :string, :default => nil

    desc 'path to a file to store last rows'
    config_param :state_file, :string, :default => nil
    desc 'prefix of tags of events. actual tag will be this_tag_prefix.tables_tag (optional)'
    config_param :tag_prefix, :string, :default => nil
    desc 'interval to run SQLs (optional)'
    config_param :select_interval, :time, :default => 60
    desc 'limit of number of rows for each SQL(optional)'
    config_param :select_limit, :time, :default => 500

    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    class TableElement
      include Configurable

      config_param :table, :string
      config_param :tag, :string, :default => nil
      config_param :update_column, :string, :default => nil
      config_param :time_column, :string, :default => nil
      config_param :primary_key, :string, :default => nil

      def configure(conf)
        super
      end

      def init(tag_prefix, base_model, router)
        @router = router
        @tag = "#{tag_prefix}.#{@tag}" if tag_prefix
        # creates a model for this table
        table_name = @table
        primary_key = @primary_key
        @model = Class.new(base_model) do
          self.table_name = table_name
          self.inheritance_column = '_never_use_'
          self.primary_key = primary_key if primary_key

          #self.include_root_in_json = false

          def read_attribute_for_serialization(n)
            v = send(n)
            if v.respond_to?(:to_msgpack)
              v
            elsif v.is_a? Time
              v.strftime('%Y-%m-%d %H:%M:%S.%6N%z')
            else
              v.to_s
            end
          end
        end

        # ActiveRecord requires model class to have a name.
        class_name = table_name.singularize.camelize
        base_model.const_set(class_name, @model)

        # Sets model_name otherwise ActiveRecord causes errors
        model_name = ActiveModel::Name.new(@model, nil, class_name)
        @model.define_singleton_method(:model_name) { model_name }

        # if update_column is not set, here uses primary key
        unless @update_column
          pk = @model.columns_hash[@model.primary_key]
          unless pk
            raise "Composite primary key is not supported. Set update_column parameter to <table> section."
          end
          @update_column = pk.name
        end
      end

      # emits next records and returns the last record of emitted records
      def emit_next_records(last_record, limit)
        relation = @model
        if last_record && last_update_value = last_record[@update_column]
          relation = relation.where("#{@update_column} > ?", last_update_value)
        end
        relation = relation.order("#{@update_column} ASC")
        relation = relation.limit(limit) if limit > 0

        now = Engine.now

        me = MultiEventStream.new
        relation.each do |obj|
          record = obj.serializable_hash rescue nil
          if record
            if @time_column && tv = obj.read_attribute(@time_column)
              if tv.is_a?(Time)
                time = tv.to_i
              else
                time = Time.parse(tv.to_s).to_i rescue now
              end
            else
              time = now
            end
            me.add(time, record)
            last_record = record
          end
        end

        last_record = last_record.dup if last_record  # some plugin rewrites record :(
        @router.emit_stream(@tag, me)

        return last_record
      end
    end

    def configure(conf)
      super

      unless @state_file
        $log.warn "'state_file PATH' parameter is not set to a 'sql' source."
        $log.warn "this parameter is highly recommended to save the last rows to resume tailing."
      end

      @tables = conf.elements.select {|e|
        e.name == 'table'
      }.map {|e|
        te = TableElement.new
        te.configure(e)
        te
      }

      if config['all_tables']
        @all_tables = true
      end
    end

    SKIP_TABLE_REGEXP = /\Aschema_migrations\Z/i

    def start
      super
      @state_store = @state_file.nil? ? MemoryStateStore.new : StateStore.new(@state_file)

      config = {
        :adapter => @adapter,
        :host => @host,
        :port => @port,
        :database => @database,
        :username => @username,
        :password => @password,
        :socket => @socket,
      }

      log.warn "adapter database '#{@adapter}'"
      log.warn "host database '#{@host}'"
      log.warn "port database '#{@port}'"
      log.warn "database database '#{@database}'"
      log.warn "username database '#{@username}'"
      log.warn "password database '#{@password}'"
      log.warn "socket database '#{@socket}'"
      # creates subclass of ActiveRecord::Base so that it can have different
      # database configuration from ActiveRecord::Base.
      @base_model = Class.new(ActiveRecord::Base) do
        # base model doesn't have corresponding phisical table
        self.abstract_class = true
      end

      # ActiveRecord requires the base_model to have a name. Here sets name
      # of an anonymous class by assigning it to a constant. In Ruby, class has
      # a name of a constant assigned first
      SQLInput.const_set("BaseModel_#{rand(1 << 31)}", @base_model)

      # Now base_model can have independent configuration from ActiveRecord::Base
      @base_model.establish_connection(config)
      

      # ignore tables if TableElement#init failed
      @tables.reject! do |te|
        begin
          log.warn "Before '#{te.table}' table"
          log.warn "Before '#{te.update_column}' column"
          te.init(@tag_prefix, @base_model, router)
          log.warn "After '#{te.table}' table"
          false
        rescue => e
          log.warn "Can't handle '#{te.table}' table. Ignoring.", :error => e.message, :error_class => e.class
          log.warn_backtrace e.backtrace
          true
        end
      log.warn "end start======"
      end

      @stop_flag = false
      log.warn "stop_flag false ============"
      @thread = Thread.new(&method(:thread_main))
      log.warn "Thread ============"
    end

    def shutdown
      @stop_flag = true
      log.warn "Waiting for thread to finish"
      @thread.join
      super
    end

    def thread_main
      until @stop_flag
        log.warn "thread main"
        sleep @select_interval
        log.warn "select interval"

        begin
          conn = @base_model.connection
          conn.active? || conn.reconnect!
        rescue => e
          log.warn "can't connect to database. Reconnect at next try"
          next
        log.warn "thread ============"
        end

        @tables.each do |t|
          begin
            log.warn "Before foreach in thread"
            last_record = @state_store.last_records[t.table]
            log.warn "Last record '#{last_record}'"
            @state_store.last_records[t.table] = t.emit_next_records(last_record, @select_limit)
            @state_store.update!
          rescue => e
            log.error "unexpected error", :error => e.message, :error_class => e.class
            log.error_backtrace e.backtrace
          end
        end
      end
      log.warn "exit thread ============"
    end

    class StateStore
      def initialize(path)
        require 'yaml'

        @path = path
        if File.exists?(@path)
          @data = YAML.load_file(@path)
          if @data == false || @data == []
            # this happens if an users created an empty file accidentally
            @data = {}
          elsif !@data.is_a?(Hash)
            raise "state_file on #{@path.inspect} is invalid"
          end
        else
          @data = {}
        end
      end

      def last_records
        @data['last_records'] ||= {}
      end

      def update!
        File.open(@path, 'w') {|f|
          f.write YAML.dump(@data)
        }
      end
    end

    class MemoryStateStore
      def initialize
        @data = {}
      end

      def last_records
        @data['last_records'] ||= {}
      end

      def update!
      end
    end
  end

end
