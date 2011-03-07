require 'active_record/connection_adapters/abstract_adapter'
require 'rbhive'
require 'json'

module ActiveRecord
  module ConnectionAdapters

    class HiveAdapter < AbstractAdapter
      attr_reader :logger
      attr_reader :connection
      cattr_accessor :dual
      @@dual = :dual

      NATIVE_DATABASE_TYPES = {
        :array       => { :name => "ARRAY<STRING>" }, 
        :text        => { :name => "STRING" },
        :datetime    => { :name => "STRING" },
        :timestamp   => { :name => "STRING" },
        :time        => { :name => "STRING" },
        :date        => { :name => "STRING" },
        :primary_key => { :name => "STRING", :null => false },
        :string      => { :name => "STRING" },
      }

      def version
        # TODO
        ""
      end

      def adapter_name
        "Hive"
      end

      def supports_migrations?
        true
      end

      def execute(query, name = nil)
        @connection.execute(query)
      end 

      def select_rows(query, name = nil)
        @connection.fetch(query)
      end


      # Maps logical Rails types to Hive-specific data types.
      def type_to_sql(type, limit = nil, precision = nil, scale = nil)
        return case limit
          when 1; 'TINYINT'
          when 2; 'SMALLINT'
          when nil, 3, 4; 'INT'
          when 5..8; 'BIGINT'
          else raise(ActiveRecordError, "No integer type has byte size #{limit}")
        end if type == :integer
        super
      end
    

      def native_database_types
        return NATIVE_DATABASE_TYPES
      end

      def select(sql, name = nil)
        results = select_rows(sql)

        # Lookup schema information
        cols = @connection.client.getSchema.fieldSchemas.collect { |c| c.name }

        rows = []
        results.each do |r|
          row = {}
          cols.each_with_index { |c, i| row[c] = r[i] }
          rows << row 
        end
        rows
      end


      def tables(name = nil)
        tables = []
        results = select_rows("SHOW TABLES")
        return results.collect { |t| t.first }
      end

      def primary_key(table)
        return columns(table).reject { |c| !c.primary }.collect { |c| c.name }
      end

      def columns(table, name = nil)
        results = select_rows("DESCRIBE EXTENDED #{table}")
        return results.delete_if { |r| r.first.blank? || r.first =~ /table info/i }.
                       collect   { |r|
          (column_name, sql_type, comment) = r
          column_details = JSON.parse(comment || "{}").symbolize_keys
          HiveColumn.new(column_name, sql_type, column_details)
        }
      end

      def add_column_options!(sql, options) #:nodoc:
        comments = options.dup.delete_if { |k, value|
          !%w(default null requested_type type partition).include?(k.to_s)
        }
        # Stuffing args we can't work with now in Hive into a comment
        sql << " COMMENT #{quote(comments.to_json)}" if comments.size > 0
      end


      def initialize_schema_migrations_table
        initialize_dual_table
        super
      end

      def initialize_dual_table
        unless table_exists?(dual)
          create_table(dual)
          execute("LOAD DATA LOCAL INPATH '/etc/hosts' OVERWRITE INTO TABLE #{dual}")
          execute("INSERT OVERWRITE TABLE #{dual} SELECT 1 FROM #{dual} LIMIT 1")
        end
      end


      def create_table(table_name, options = {})
        td = hive_table_definition
        td.primary_key(options[:primary_key] || Base.get_primary_key(table_name.to_s.singularize)) unless options[:id] == false

        yield td if block_given?

        if options[:force] && table_exists?(table_name)
          drop_table(table_name, options)
        end

        create_sql = "CREATE "
        create_sql << "TEMPORARY "  if options[:temporary]
        create_sql << "EXTERNAL "   if options[:external]
        create_sql << "TABLE #{quote_table_name(table_name)} ("
        create_sql << td.to_sql
        create_sql << ")"
        if td.partitions.size > 0
          create_sql << "PARTITIONED BY ("
          create_sql << td.partitions_to_sql
          create_sql << ")"
        end
        create_sql << "#{options[:options]}"
        execute create_sql
      end







      # TODO Deal with Hive 0.7 Indexes
      def indexes(table_name, name = nil); []; end
      def indexes_not_supported(*args)
        logger.fatal(<<"        INDEXES_NOT_SUPPORTED")
          Indexes aren't supported in #{self.adapter_name} on version #{version}
          So #{caller.first} on #{args.to_json} would fail
        INDEXES_NOT_SUPPORTED
      end
      def index_name_exists?(table_name, index_name, default); false; end
      def index_name(table_name, options);                  indexes_not_supported; end
      def remove_index!(table_name, index_name);            indexes_not_supported; end
      def remove_index(table_name, options = {});           indexes_not_supported; end
      def rename_index(table_name, old_name, new_name);     indexes_not_supported; end
      def add_index(table_name, column_name, options = {}); indexes_not_supported; end
      def index_exists?(table_name, column_name, options = {}); indexes_not_supported; end



      def hive_table_definition
        HiveTableDefinition.new(self)
      end
    end

    class HiveColumn < Column
      attr_accessor :partition

      def initialize(name, sql_type, column_details)
        super(name, column_details[:default], sql_type, 
              column_details[:null].nil? ? true : column_details[:null])
        self.primary   = true if column_details[:requested_type] == 'primary_key'
        self.partition = true if column_details[:partition]
      end

      def klass
        return type == :array ? Array : super
      end

      def simplified_type(field_type)
        return field_type =~ /array/i ? :array : super
      end
    end

    class ColumnDefinition
      attr_accessor :partition

      def to_sql
        column_sql = "#{base.quote_column_name(name)} #{sql_type}"
        column_options = {}
        column_options[:null] = null unless null.nil?
        column_options[:default] = default unless default.nil?
        column_options[:partition] = partition if partition
        column_options[:requested_type] = type
        add_column_options!(
          column_sql, 
          base.native_database_types[type].update(column_options)
        )
        column_sql
      end

      

    end

    class HiveTableDefinition < TableDefinition
     
      def initialize(base)
        @partitions = []
        super
      end 

      def partitions
        return columns.reject { |c| !c.partition }
      end

      def column(name, type, options = {})
        super
        c = self[name]
        c.partition = options[:partition]
        self
      end

      def to_sql
        columns.reject { |c| c.partition }.map { |c| c.to_sql } * ', '
      end

      def partitions_to_sql
        partitions.collect { |c| c.to_sql } * ', '
      end
    end

  end

  class Base
    def self.hive_connection(config)
      connection = RBHive::Connection.new(config[:host], config[:port] || 10_000, logger)
      connection.open
      ConnectionAdapters::HiveAdapter.new(connection, logger)
    end
  end
end

