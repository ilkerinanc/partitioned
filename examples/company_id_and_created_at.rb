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
require File.expand_path(File.dirname(__FILE__) + "/lib/company")
require File.expand_path(File.dirname(__FILE__) + "/lib/by_company_id")

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
#  employees_partitions.p1_20101227
#  employees_partitions.p1_20110103
#  employees_partitions.p1_20110110
#  employees_partitions.p1_20110117
#  employees_partitions.p1_20110124
#  employees_partitions.p1_20110131
#  employees_partitions.p1_20110207
#  employees_partitions.p1_20110214
#  employees_partitions.p1_20110221
#  employees_partitions.p1_20110228
#  employees_partitions.p1_20110307
#  employees_partitions.p1_20110314
#  employees_partitions.p1_20110321
#  employees_partitions.p1_20110328
#  employees_partitions.p1_20110404
#  employees_partitions.p1_20110411
#  employees_partitions.p1_20110418
#  employees_partitions.p1_20110425
#  employees_partitions.p1_20110502
#  employees_partitions.p1_20110509
#  employees_partitions.p1_20110516
#  employees_partitions.p1_20110523
#  employees_partitions.p1_20110530
#  employees_partitions.p1_20110606
#  employees_partitions.p1_20110613
#  employees_partitions.p1_20110620
#  employees_partitions.p1_20110627
#  employees_partitions.p1_20110704
#  employees_partitions.p1_20110711
#  employees_partitions.p1_20110718
#  employees_partitions.p1_20110725
#  employees_partitions.p1_20110801
#  employees_partitions.p1_20110808
#  employees_partitions.p1_20110815
#  employees_partitions.p1_20110822
#  employees_partitions.p1_20110829
#  employees_partitions.p1_20110905
#  employees_partitions.p1_20110912
#  employees_partitions.p1_20110919
#  employees_partitions.p1_20110926
#  employees_partitions.p1_20111003
#  employees_partitions.p1_20111010
#  employees_partitions.p1_20111017
#  employees_partitions.p1_20111024
#  employees_partitions.p1_20111031
#  employees_partitions.p1_20111107
#  employees_partitions.p1_20111114
#  employees_partitions.p1_20111121
#  employees_partitions.p1_20111128
#  employees_partitions.p1_20111205
#  employees_partitions.p1_20111212
#  employees_partitions.p1_20111219
#  employees_partitions.p1_20111226
#  For the next three lines the similar partitions are generated.
#  Difference only in company_id prefix.
#  employees_partitions.p2_20101227 - employees_partitions.p2_20111226
#  employees_partitions.p3_20101227 - employees_partitions.p3_20111226
#  employees_partitions.p4_20101227 - employees_partitions.p4_20111226

# now add some employees across the year.

employees = []

require File.expand_path(File.dirname(__FILE__) + "/lib/roman")

# generates data for employees_partitions and employees tables

(1..NUM_EMPLOYEES).each do |i|
  employees << {
    :name => "Winston J. Sillypants, #{to_roman(i)}",
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
end

Employee.create_many(employees)
