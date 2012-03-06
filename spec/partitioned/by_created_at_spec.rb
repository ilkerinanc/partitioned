require 'spec_helper'
require "#{File.dirname(__FILE__)}/../support/tables_spec_helper"
require "#{File.dirname(__FILE__)}/../support/shared_example_spec_helper_for_time_key"

module Partitioned

  describe ByCreatedAt do

    include TablesSpecHelper

    module CreatedAt
      class Employee < Partitioned::ByCreatedAt
        belongs_to :company, :class_name => 'Company'

        partitioned do |partition|
          partition.index :id, :unique => true
          partition.foreign_key :company_id
        end
      end # Employee
    end # CreatedAt

    before(:all) do
      @employee = CreatedAt::Employee
      create_tables
      dates = @employee.partition_generate_range(DATE_NOW,
                                                 DATE_NOW + 7.days)
      @employee.create_new_partition_tables(dates)
      ActiveRecord::Base.connection.execute <<-SQL
        insert into employees_partitions.
          p#{DATE_NOW.at_beginning_of_week.strftime('%Y%m%d')}
          (company_id,name) values (1,'Keith');
      SQL
    end

    after(:all) do
      drop_tables
    end

    let(:class_by_created_at) { ::Partitioned::ByCreatedAt }

    describe "model is abstract class" do

      it "returns true" do
        class_by_created_at.abstract_class.should be_true
      end

    end # model is abstract class

    describe "#partition_time_field" do

      it "returns :created_at" do
        class_by_created_at.partition_time_field.should == :created_at
      end

    end # #partition_time_field

    it_should_behave_like "check that basic operations with postgres works correctly for time key", CreatedAt::Employee

  end # ByCreatedAt

end # Partitioned