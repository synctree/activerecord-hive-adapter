require 'active_record/connection_adapters/abstract_adapter'
require 'rbhive'

module ActiveRecord
  module ConnectionAdapters
    class HiveAdapter < AbstractAdapter
      attr_reader :logger
      cattr_accessor :dual
      @@dual = :dual

      NATIVE_DATABASE_TYPES = {
        :text        => { :name => "STRING" },
        :datetime    => { :name => "STRING" },
        :timestamp   => { :name => "STRING" },
        :time        => { :name => "STRING" },
        :date        => { :name => "STRING" },
        :primary_key => { :name => "STRING" },
      }

      def hive_version
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
          when 1; 'tinyint'
          when 2; 'smallint'
          when nil, 3, 4; 'int'
          when 5..8; 'bigint'
          else raise(ActiveRecordError, "No integer type has byte size #{limit}")
        end if type == :integer

        sql = super
        # Hack to get around Column Definition not including column info in primary
        # key def
        add_column_options!(
          sql,
          native_database_types[type].update(:requested_type => type)
        )

        return sql
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
        return columns(table).delete_if { |c| !c.primary }.collect { |c| c.name }
      end

      def columns(table, name = nil)
        results = select_rows("DESCRIBE EXTENDED #{table}")
        # NOTE if this code gets too long just create a custom HiveColumn class
        columns = []
        return results. delete_if { |r| r.first.blank? || r.first =~ /table info/i }.
                        collect   { |r|
          (column_name, type, comment) = r

          column_details = {}
          begin
            column_details = JSON.parse(comment).symbolize_keys
          rescue;
          end

          c = Column.new(column_name, column_details[:default], type, column_details[:null])
          c.primary = true if column_details[:requested_type] == 'primary_key'
          c
        }
      end

      def add_column_options!(sql, options) #:nodoc:
        comments = options.dup.delete_if { |k, value|
          !%w(default null requested_type type).include?(k.to_s)
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








      # TODO Deal with Hive 0.7 Indexes
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

    end
  end

  class Base
    def self.hive_connection(config)
      connection = RBHive::Connection.new(config[:host], config[:port] || 10_000)
      connection.open
      ConnectionAdapters::HiveAdapter.new(connection, logger)
    end
  end
end

