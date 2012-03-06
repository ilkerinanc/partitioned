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
require 'lib/by_company_id'

class Employee < Partitioned::MultiLevel
  belongs_to :company, :class_name => 'Company'

  partitioned do |partition|
    partition.using_classes ByCompanyId, Partitioned::ByCreatedAt
  end

  connection.execute <<-SQL
    create table employees
    (
        id               serial not null primary key,
        created_at       timestamp not null default now(),
        updated_at       timestamp,
        name             text not null,
        salary           money not null,
        company_id       integer not null
    );
  SQL
end

# You should have the following tables:
#  public.companies
#  public.employees

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)
dates = Partitioned::ByCreatedAt.partition_generate_range(START_DATE, END_DATE)

partition_key_values = []
company_ids.each do |company_id|
  partition_key_values << company_id
  dates.each do |date|
    partition_key_values << [company_id, date]
  end
end

# create the infrastructure for EMPLOYEES table which includes the schema and partition tables

Employee.create_infrastructure([] + partition_key_values)

# You should have the following schema:
#  employees_partitions

Employee.create_new_partition_tables(partition_key_values)

# You should have the following tables:
#  employees_partitions.p1
#  employees_partitions.p2
#  employees_partitions.p3
#  employees_partitions.p4
#  employees_partitions.p1_20110101 - employees_partitions.p1_20111230
#  employees_partitions.p2_20110101 - employees_partitions.p2_20111230
#  employees_partitions.p3_20110101 - employees_partitions.p3_20111230
#  employees_partitions.p4_20110101 - employees_partitions.p4_20111230

# now add some employees across the year.

employees = []

require 'lib/roman'

(1..NUM_EMPLOYEES).each do |i|
  employees << {
    :name => "Winston J. Sillypants, #{to_roman(i)}",
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
end

Employee.create_many(employees)
