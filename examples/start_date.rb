#!/usr/bin/env ../../../../script/rails runner
# if you use linux, please change previous line to the
# "#!../../../../script/rails runner"

if ['--cleanup', '--force'].include?(ARGV[0])
  ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("employees") rescue nil
  ActiveRecord::Base.connection.drop_table("companies") rescue nil
  exit(0) if ARGV[0] == '--cleanup'
end

START_DATE = Date.parse('2011-01-01')
END_DATE = Date.parse('2011-12-31')
NUM_EMPLOYEES = 5000

# the ActiveRecord classes

require 'lib/company'

class Partitioned::ByStartDate < Partitioned::ByMonthlyTimeField
  self.abstract_class = true

  def self.partition_time_field
    return :start_date
  end
end

class Employee < Partitioned::ByStartDate
  belongs_to :company, :class_name => 'Company'

  partitioned do |partition|
    partition.index :id, :unique => true
    partition.foreign_key :company_id
  end

  connection.execute <<-SQL
    create table employees
    (
        id               serial not null primary key,
        created_at       timestamp not null default now(),
        updated_at       timestamp null,
        start_date       date not null,
        name             text not null,
        salary           money not null,
        company_id       integer not null
    );
  SQL
end

# You should have the following tables:
#  public.companies
#  public.employees

# create the infrastructure for EMPLOYEES table which includes the schema and partition tables

Employee.create_infrastructure

# You should have the following schema:
#  employees_partitions

dates = Employee.partition_generate_range(START_DATE, END_DATE)
Employee.create_new_partition_tables(dates)

# You should have the following tables with increments of one month:
#  employees_partitions.p201101 - employees_partitions.p201112

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)

# now add some employees across the year.

employees = []

require 'lib/roman'

# generates data for employees_partitions and employees tables

(1..NUM_EMPLOYEES).each do |i|
  employees << {
    :name => "Winston J. Sillypants, #{to_roman(i)}",
    :start_date => START_DATE + rand(END_DATE - START_DATE),
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
end

Employee.create_many(employees)
