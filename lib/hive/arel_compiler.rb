module Arel
  module SqlCompiler
    class HiveCompiler < GenericCompiler

      # TODO remove this when Hive supports real inserts because this is SUPER DUPER
      # SLOW
      def insert_sql(include_returning = true)
        insertion_attributes_values_sql = if relation.record.is_a?(Value)
          relation.record.value
        else
          record_keys = {}
          relation.record.keys.each { |key| record_keys[key.name.to_s] = key }
          build_query relation.table.columns.collect { |c|
            key = record_keys[c.name]
            value = relation.record[key] ? key.format(relation.record[key]) : "NULL"
            value = "CAST(NULL AS #{c.sql_type})" if value == "NULL"
            "#{value} AS #{c.name}"
          }.join(",")
        end

        # TODO build the query portion of this using Arel
        build_query \
          "INSERT OVERWRITE TABLE #{relation.table_sql}",
          "SELECT * FROM (",
            "SELECT * FROM #{relation.table_sql}",
            "UNION ALL",
            "SELECT",
            insertion_attributes_values_sql,
            "FROM #{relation.engine.connection.dual}",
            "LIMIT 1",
          ") t"
      end

    end
  end
end
