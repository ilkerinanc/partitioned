require 'spec_helper'
require "#{File.dirname(__FILE__)}/../support/tables_spec_helper"
require "#{File.dirname(__FILE__)}/../support/shared_example_spec_helper_for_time_key"

module Partitioned

  describe ByTimeField do

    include TablesSpecHelper

    module TimeField
      class Employee < Partitioned::ByTimeField
        belongs_to :company, :class_name => 'Company'

        def self.partition_time_field
          return :created_at
        end

        partitioned do |partition|
          partition.index :id, :unique => true
          partition.foreign_key :company_id
        end
      end # Employee
    end # TimeField

    before(:all) do
      @employee = TimeField::Employee
      create_tables
      dates = @employee.partition_generate_range(DATE_NOW,
                                                 DATE_NOW + 1.day)
      @employee.create_new_partition_tables(dates)
      ActiveRecord::Base.connection.execute <<-SQL
        insert into employees_partitions.
          p#{DATE_NOW.strftime('%Y%m%d')}
          (company_id,name) values (1,'Keith');
      SQL
    end

    after(:all) do
      drop_tables
    end

    let(:class_by_time_field) { ::Partitioned::ByTimeField }

    describe "model is abstract class" do

      it "returns true" do
        class_by_time_field.abstract_class.should be_true
      end

    end # model is abstract class

    describe "#partition_generate_range" do

      it "returns dates array" do
        class_by_time_field.
            partition_generate_range(Date.parse('2011-01-05'), Date.parse('2011-01-07')).
            should == [Date.parse('2011-01-05'), Date.parse('2011-01-06'), Date.parse('2011-01-07')]
      end

    end # #partition_generate_range

    describe "#partition_normalize_key_value" do

      it "returns date" do
        class_by_time_field.
            partition_normalize_key_value(Date.parse('2011-01-05')).
            should == Date.parse('2011-01-05')
      end

    end # #partition_normalize_key_value

    describe "#partition_table_size" do

      it "returns 1.day" do
        class_by_time_field.partition_table_size.should == 1.day
      end

    end # #partition_table_size

    describe "#partition_time_field" do

      it "raises MethodNotImplemented" do
        lambda {
          class_by_time_field.partition_time_field
        }.should raise_error(MethodNotImplemented)
      end

    end # #partition_time_field

    describe "partitioned block" do

      let(:data) do
        class_by_time_field.configurator_dsl.data
      end

      context "checks data in the on_field is Proc" do

        it "returns Proc" do
          data.on_field.should be_is_a Proc
        end

      end # checks data in the on_field is Proc

      context "checks data in the indexes is Proc" do

        it "returns Proc" do
          data.indexes.first.should be_is_a Proc
        end

      end # checks data in the indexes is Proc

      context "checks data in the base_name is Proc" do

        it "returns Proc" do
          data.base_name.should be_is_a Proc
        end

      end # checks data in the base_name is Proc

      context "checks data in the check_constraint is Proc" do

        it "returns Proc" do
          data.check_constraint.should be_is_a Proc
        end

      end # checks data in the check_constraint is Proc

      context "checks data in the on_field" do

        it "returns on_field" do
          data.on_field.call(@employee).should == :created_at
        end

      end # checks data in the on_field

      context "checks data in the indexes" do

        it "returns :created_at" do
          data.indexes.first.call(@employee, nil).field.should == :created_at
        end

        it "returns empty options hash" do
          data.indexes.first.call(@employee, nil).options.should == {}
        end

      end # checks data in the indexes

      context "checks data in the last_partitions_order_by_clause" do

        it "returns last_partitions_order_by_clause" do
          data.last_partitions_order_by_clause.should == "tablename desc"
        end

      end # checks data in the last_partitions_order_by_clause

      context "checks data in the base_name" do

        it "returns base_name" do
          data.base_name.call(@employee, Date.parse('2011-01-05')).should == "20110105"
        end

      end # checks data in the base_name

      context "checks data in the check_constraint" do

        it "returns check_constraint" do
          data.check_constraint.
              call(@employee, Date.parse('2011-01-05')).
              should == "created_at >= '2011-01-05' AND created_at < '2011-01-06'"
        end

      end # checks data in the check_constraint

    end # partitioned block

    it_should_behave_like "check that basic operations with postgres works correctly for time key", TimeField::Employee

  end # ByTimeField

end # Partitioned