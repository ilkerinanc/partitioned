require 'spec_helper'
require "#{File.dirname(__FILE__)}/../support/tables_spec_helper"
require "#{File.dirname(__FILE__)}/../support/shared_example_spec_helper_for_integer_key"

module Partitioned

  describe ById do

    include TablesSpecHelper

    module Id
      class Employee < ById
        belongs_to :company, :class_name => 'Company'
        attr_accessible :company_id, :name, :integer_field

        def self.partition_table_size
          return 1
        end

        partitioned do |partition|
          partition.foreign_key :company_id
        end
      end # Employee
    end # Id

    before(:all) do
      @employee = Id::Employee
      create_tables
      @employee.create_new_partition_tables(Range.new(1, 5).step(@employee.partition_table_size))
      ActiveRecord::Base.connection.execute <<-SQL
        insert into employees_partitions.p1 (company_id,name) values (1,'Keith');
      SQL
    end

    after(:all) do
      drop_tables
    end

    let(:class_by_id) { ::Partitioned::ById }

    describe "model is abstract class" do

      it "returns true" do
        class_by_id.abstract_class.should be_true
      end

    end # model is abstract class

    describe "#prefetch_primary_key?" do

      context "is :id set as a primary_key" do

        it "returns true" do
          class_by_id.prefetch_primary_key?.should be_true
        end

      end # is :id set as a primary_key

    end # #prefetch_primary_key?

    describe "#partition_table_size" do

      it "returns 10000000" do
        class_by_id.partition_table_size.should == 10000000
      end

    end # #partition_table_size

    describe "#partition_integer_field" do

      it "returns :id" do
        class_by_id.partition_integer_field.should == :id
      end

    end # #partition_integer_field

    describe "partitioned block" do

      context "checks if there is data in the indexes field" do

        it "returns :id" do
          class_by_id.configurator_dsl.data.indexes.first.field.should == :id
        end

        it "returns { :unique => true }" do
          class_by_id.configurator_dsl.data.indexes.first.options.should == { :unique => true }
        end

      end # checks if there is data in the indexes field

    end # partitioned block

    it_should_behave_like "check that basic operations with postgres works correctly for integer key", Id::Employee

  end # ById

end # Partitioned