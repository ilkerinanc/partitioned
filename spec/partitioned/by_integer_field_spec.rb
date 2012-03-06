require 'spec_helper'
require "#{File.dirname(__FILE__)}/../support/tables_spec_helper"
require "#{File.dirname(__FILE__)}/../support/shared_example_spec_helper_for_integer_key"

module Partitioned

  describe ByIntegerField do

    include TablesSpecHelper

    module IntegerField
      class Employee < ByIntegerField
        belongs_to :company, :class_name => 'Company'

        def self.partition_integer_field
          return :id
        end

        partitioned do |partition|
          partition.foreign_key :company_id
        end
      end # Employee
    end # IntegerField

    before(:all) do
      @employee = IntegerField::Employee
      create_tables
      @employee.create_new_partition_tables(Range.new(1, 3).step(@employee.partition_table_size))
      ActiveRecord::Base.connection.execute <<-SQL
        insert into employees_partitions.p1 (company_id,name) values (1,'Keith');
      SQL
    end

    after(:all) do
      drop_tables
    end

    let(:class_by_integer_field) { ::Partitioned::ByIntegerField }

    describe "model is abstract class" do

      it "returns true" do
        class_by_integer_field.abstract_class.should be_true
      end

    end # model is abstract class

    describe "#partition_table_size" do

      it "returns 1" do
        class_by_integer_field.partition_table_size.should == 1
      end

    end # #partition_table_size

    describe "#partition_integer_field" do

      it "raises MethodNotImplemented" do
        lambda {
          class_by_integer_field.partition_integer_field
        }.should raise_error(MethodNotImplemented)
      end

    end # #partition_integer_field

    describe "#partition_normalize_key_value" do

      context "when call method with param equal five" do

        it "returns 5" do
          class_by_integer_field.partition_normalize_key_value(5).should == 5
        end

      end

    end # #partition_normalize_key_value

    describe "partitioned block" do

      let(:data) do
        class_by_integer_field.configurator_dsl.data
      end

      context "checks data in the on_field is Proc" do

        it "returns Proc" do
          data.on_field.should be_is_a Proc
        end

      end # checks data in the on_field is Proc

      context "checks data in the check_constraint is Proc" do

        it "returns Proc" do
          data.check_constraint.should be_is_a Proc
        end

      end # checks data in the check_constraint is Proc

      context "checks data in the on_field" do

        it "returns on_field" do
          data.on_field.call(@employee).should == :id
        end

      end # checks data in the on_field

      context "checks data in the last_partitions_order_by_clause" do

        it "returns last_partitions_order_by_clause" do
          data.last_partitions_order_by_clause.should == "substring(tablename, 2)::integer desc"
        end

      end # checks data in the last_partitions_order_by_clause

      context "checks data in the check_constraint" do

        it "returns check_constraint" do
          data.check_constraint.call(@employee, 1).should == "( id = 1 )"
        end

      end # checks data in the check_constraint

      context "checks data in the check_constraint, when partition_table_size != 1" do

        before do
          @employee.stub!(:partition_table_size).and_return(2)
        end

        it "returns check_constraint" do
          data.check_constraint.call(@employee, 1).should == "( id >= 0 and id < 2 )"
        end

      end # checks data in the check_constraint, when partition_table_size != 1

    end # partitioned block

    it_should_behave_like "check that basic operations with postgres works correctly for integer key", IntegerField::Employee

  end # ByIntegerField

end # Partitioned
