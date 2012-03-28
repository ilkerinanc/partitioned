require 'spec_helper'

module Partitioned
  describe PartitionedBase do

    before(:all) do
      class Employee < PartitionedBase
      end
    end

    after(:all) do
      Partitioned.send(:remove_const, :Employee)
    end

    let(:class_partitioned_base) { ::Partitioned::PartitionedBase }

    describe "model is abstract class" do

      it "returns true" do
        class_partitioned_base.abstract_class.should be_true
      end

    end # model is abstract class

    describe "partitioned block" do

      let(:data) do
        class_partitioned_base.configurator_dsl.data
      end

      context "checks data in the schema_name" do

        it "returns schema_name" do
          data.schema_name.call(Employee).should == "employees_partitions"
        end

      end # checks data in the schema_name

      context "checks data in the parent_table_name" do

        it "returns parent_table_name" do
          data.parent_table_name.call(Employee).should == "employees"
        end

      end # checks data in the parent_table_name

      context "checks data in the parent_table_schema_name" do

        it "returns parent_table_schema_name" do
          data.parent_table_schema_name.call(Employee).should == "public"
        end

      end # checks data in the parent_table_schema_name

      context "checks data in the name_prefix" do

        it "returns name_prefix" do
          data.name_prefix.call(Employee).should == "p"
        end

      end # checks data in the name_prefix

      context "checks data in the part_name" do

        it "returns part_name" do
          data.part_name.call(Employee, 1).should == "p1"
        end

      end # checks data in the part_name

      context "checks data in the table_name" do

        it "returns table_name" do
          data.table_name.call(Employee, 1).should == "employees_partitions.p1"
        end

      end # checks data in the table_name

      context "checks data in the base_name" do

        it "returns base_name" do
          data.base_name.call(Employee, 1).should == "1"
        end

      end # checks data in the base_name
    end # partitioned block

    context "#partition_key_values" do

      before do
        class_partitioned_base.stub!(:partition_keys).and_return([:id])
      end

      context "call method with key that represented as a string" do

        it "returns values" do
          class_partitioned_base.partition_key_values( "id" => 1 ).should == [1]
        end

      end # call method with key that represented as a string

      context "call method with key that represented as a symbol" do

        it "returns values" do
          class_partitioned_base.partition_key_values( :id => 2 ).should == [2]
        end

      end # call method with key that represented as a symbol

    end # #partition_key_values

    context "checks instance methods" do

      before do
        ActiveRecord::Base.connection.execute <<-SQL
          create table employees (
            id               serial not null primary key,
            created_at       timestamp not null default now(),
            updated_at       timestamp,
            name             text not null
          );
        SQL
        Employee.stub!(:partition_keys).and_return([:id])
        @employee = Employee.new
      end

      context "partition_table_name" do

        context "call method with attributes key that represented as a string" do

          it "returns employees_partitions.p1" do
            @employee.stub!(:attributes).and_return("id" => 1)
            @employee.partition_table_name.should == "employees_partitions.p1"
          end

        end # call method with attributes key that represented as a string

        context "call method with attributes key that represented as a symbol" do

          it "returns employees_partitions.p2" do
            @employee.stub!(:attributes).and_return(:id => 2)
            @employee.partition_table_name.should == "employees_partitions.p2"
          end

        end # call method with attributes key that represented as a symbol

      end # partition_table_name

      context "dynamic_arel_table" do

        context "call method with attributes key that represented as a string" do

          it "returns arel table name employees_partitions.p1" do
            @employee.stub!(:attributes).and_return("id" => 1)
            @employee.dynamic_arel_table.name.should == "employees_partitions.p1"
          end

        end # call method with attributes key that represented as a string

        context "call method with attributes key that represented as a symbol" do

          it "returns arel table name employees_partitions.p2" do
            @employee.stub!(:attributes).and_return(:id => 2)
            @employee.dynamic_arel_table.name.should == "employees_partitions.p2"
          end

        end # call method with attributes key that represented as a symbol

      end # dynamic_arel_table

    end # checks instance methods
  end # PartitionedBase
end # Partitioned