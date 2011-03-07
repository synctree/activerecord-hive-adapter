require 'spec_helper'

describe "HiveAdapter" do
  include SchemaSpecHelper

  before(:all) do
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

  describe "create_table" do
    before(:all) do
    schema_define do
      create_table :with_an_array_column do |t|
        t.column :c, :array
      end 
    end 
    end

    it "should default :id column type to string" do
    schema_define do
      c = column_for(:id, :with_an_array_column)
      c.sql_type.upcase.should == "STRING"
    end
    end

    it "should make :array column type to ARRAY<STRING>" do
    schema_define do
      c = column_for(:c, :with_an_array_column)
      c.sql_type.upcase.should == "ARRAY<STRING>"
    end  
    end
  end

end
