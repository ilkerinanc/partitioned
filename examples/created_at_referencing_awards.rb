#!/usr/bin/env ../../../../script/rails runner
# if you use linux, please change previous line to the
# "#!../../../../script/rails runner"

if ['--cleanup', '--force'].include?(ARGV[0])
  ActiveRecord::Base.connection.drop_schema("awards_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("awards") rescue nil
  ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("employees") rescue nil
  ActiveRecord::Base.connection.drop_table("companies") rescue nil
  exit(0) if ARGV[0] == '--cleanup'
end

START_DATE = Date.parse('2011-01-01')
END_DATE = Date.parse('2011-12-31')
NUM_EMPLOYEES = 5000
NUM_AWARDS = NUM_EMPLOYEES / 10

# the ActiveRecord classes

require 'lib/company'

class Employee < Partitioned::ByCreatedAt
  belongs_to :company, :class_name => 'Company'
  belongs_to :region, :class_name => 'Region'

  partitioned do |partition|
    partition.index :id, :unique => true
    partition.foreign_key :company_id
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

class ByEmployeeCreatedAt < Partitioned::ByWeeklyTimeField
  self.abstract_class = true

  def self.partition_time_field
    return :employee_created_at
  end
end

class Award < ByEmployeeCreatedAt
  belongs_to :employee, :class_name => 'Employee'

  partitioned do |partition|
    partition.foreign_key lambda {|model, *partition_key_values|
      return Configurator::Data::ForeignKey.new(:employee_id, Employee.partition_name(*partition_key_values), :id)
    }
  end
  
  connection.execute <<-SQL
    create table awards
    (
        id                    serial not null primary key,
        created_at            timestamp not null default now(),
        employee_created_at   timestamp not null,
        awarded_on            date not null,
        employee_id           integer not null,
        award_title           text not null
    );
  SQL
end

# You should have the following tables:
#  public.companies
#  public.employees

# create the infrastructure for EMPLOYEES table which includes the schema and partition tables

Employee.create_infrastructure

# create the infrastructure for Awards table which includes the schema and partition tables

Award.create_infrastructure

# You should have the following schema:
#  employees_partitions
#  awards_partitions

dates = Employee.partition_generate_range(START_DATE, END_DATE)

Employee.create_new_partition_tables(dates)

# You should have the following tables with increments of one week:
#   employees_partitions.p20101227 - employees_partitions.p20111226

Award.create_new_partition_tables(dates)

# You should have the following tables with increments of one week:
#   awards_partitions.p20101227 - awards_partitions.p20111226

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
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
end

Employee.create_many(employees)

# generates data for awards_partitions and awards tables

awards = []
(1..NUM_AWARDS).each do |i|
  employee = Employee.find(rand(NUM_EMPLOYEES))
  awards << {
    :award_title => "You're the best #{to_roman(i)}",
    :awarded_on => employee.created_at + rand(1.year.seconds).seconds,
    :employee_created_at => employee.created_at,
    :employee_id => employee.id
  }
end

Award.create_many(awards)
