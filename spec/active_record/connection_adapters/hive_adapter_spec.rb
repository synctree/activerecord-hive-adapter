require 'spec_helper'

describe "HiveAdapter" do
  include SchemaSpecHelper

  before(:all) do
    ActiveRecord::Base.logger = Logger.new(STDERR)
    ActiveRecord::Base.establish_connection(
      :adapter => 'hive',
      :host    => 'localhost', 
      :port    => 10_000
    )
  end
  after(:all) do
    schema_define do
      # drop all tables
      tables.each { |t| drop_table t }
    end
  end

  it "should pass logger through to rbhive connection" do
    ActiveRecord::Base.connection.logger.should == ActiveRecord::Base.connection.connection.instance_variable_get("@logger")
  end

  describe "create_table" do
    before(:all) do
    schema_define do
      create_table :partitioned_with_an_array_column do |t|
        t.column :c, :array
        t.column :partition_a, :string, :partition => true, :null => false
      end 
    end 
    end

    it "should classify partition columns" do
      schema_define do
        c = column_for(:partition_a, :partitioned_with_an_array_column)
        c.sql_type.upcase.should == "STRING"
        c.type.should == :string
        c.partition.should == true
        c.null.should == false
      end
    end

    it "should default :id column type to string" do
    schema_define do
      c = column_for(:id, :partitioned_with_an_array_column)
      c.sql_type.upcase.should == "STRING"
      c.type.should == :string
      c.primary.should == true
    end
    end

    it "should convert :array column type to ARRAY<STRING>" do
    schema_define do
      c = column_for(:c, :partitioned_with_an_array_column)
      c.sql_type.upcase.should == "ARRAY<STRING>"
      c.type.should == :array
      c.null.should == true
    end  
    end
  end

end
