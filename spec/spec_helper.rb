$:.unshift(File.expand_path('../../lib', __FILE__))
require 'active_record'
require 'active_record/connection_adapters/hive_adapter'
require 'ruby-debug'

module SchemaSpecHelper
  def schema_define(&block)
    ActiveRecord::Schema.define do
      instance_eval(&block)
    end
  end
end

class ActiveRecord::ConnectionAdapters::HiveAdapter
  def column_for(column_name, table_name)
    return columns(table_name).delete_if { |c| c.name != column_name.to_s }.first
  end
end
