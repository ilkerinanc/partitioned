require 'spec_helper'

module Partitioned
  class MultiLevel
    module Configurator
      describe Reader do

        before(:all) do
          class Partitioned::ById
            def self.partition_field
              :id
            end
            def self.partition_table_size
              return 1
            end
          end
          class Partitioned::ByCreatedAt
            def self.partition_field
              :created_at
            end
          end
          class Employee < MultiLevel
            def self.first_partition_field
              :id
            end
            def self.second_partition_field
              :created_at
            end
            partitioned do |partition|
              partition.index :id, :unique => true
              partition.using_classes Partitioned::ById, Partitioned::ByCreatedAt
            end
          end
        end

        after(:all) do
          Partitioned::MultiLevel::Configurator.send(:remove_const, :Employee)
        end

        let!(:reader) do
          Partitioned::MultiLevel::Configurator::Reader.new(Employee)
        end

        describe "using_configurators" do

          context "checking arrays length" do

            it "returns 6" do
              reader.send(:using_configurators).length.should == 6
            end

          end # checking array length

          context "checking models class for each of using_configurators" do

            it "returns 'Partitioned::ById'" do
              for i in 0..2
                reader.send(:using_configurators)[i].model.to_s.should == "Partitioned::ById"
              end
            end

            it "returns 'Partitioned::ByCreatedAt'" do
              for i in 3..5
                reader.send(:using_configurators)[i].model.to_s.should == "Partitioned::ByCreatedAt"
              end
            end

          end # checking models class for each of using_configurators

        end # using_configurators

        describe "on_fields" do

          context "when on_field value is set by default" do

            it "returns [:id, :created_at]" do
              reader.on_fields.sort{|a,b| a.to_s <=> b.to_s}.
                  should == [:id, :created_at].sort{|a,b| a.to_s <=> b.to_s}
            end

          end # when on_filed value is set by default

        end # on_fields

        describe "parent_table_schema_name" do

          context "when parent_table_schema_name value is set without options" do

            it "returns public" do
              reader.parent_table_schema_name.should == "public"
            end

          end # when parent_table_schema_name value is set by default

          context "when parent_table_schema_name value is set with options" do

            it "returns employees_partitions" do
              reader.parent_table_schema_name(1, Date.parse("2011-01-03")).should == "employees_partitions"
            end

          end # when parent_table_schema_name value is set by value

        end # parent_table_schema_name

        describe "parent_table_name" do

          context "when parent_table_name value is set without options" do

            it "returns employees" do
              reader.parent_table_name.should == "employees"
            end

          end # when parent_table_name value is set without options

          context "when parent_table_name value is set with options" do

            it "returns employees_partitions.p0" do
              reader.parent_table_name(1, Date.parse("2011-01-03")).should == "employees_partitions.p1"
            end

          end # when parent_table_name value is set with options

        end # parent_table_name

        describe "check_constraint" do

          it "returns check_constraint" do
            reader.check_constraint(1).
                should == "( id = 1 )"
            reader.check_constraint(1, Date.parse('2011-10-10')).
                should == "created_at >= '2011-10-10' AND created_at < '2011-10-17'"
          end

        end # check_constraint

        describe "base_name" do

          it "returns base_name" do
            reader.base_name(1, Date.parse('2011-10-10')).should == "1_20111010"
          end

        end # base_name

      end # Reader
    end # Configurator
  end # MultiLevel
end # Partitioned