#!/usr/bin/env ../../../../script/rails runner
# if you use linux, please change previous line to the
# "#!../../../../script/rails runner"

require File.expand_path(File.dirname(__FILE__) + "/lib/command_line_tool_mixin")
require File.expand_path(File.dirname(__FILE__) + "/lib/get_options")

include CommandLineToolMixin

$cleanup = false
$force = false
$create_many = 1500
$create_individual = 500
$new_individual = 500
$update_many = 500
$update_individual = 500

@options = {
  "--cleanup" => {
    :short => "-C",
    :argument => GetoptLong::NO_ARGUMENT,
    :note => "cleanup data in database and exit"
  },
  "--force" => {
    :short => "-F",
    :argument => GetoptLong::NO_ARGUMENT,
    :note => "cleanup data in database before creating new data"
  },
  "--create-many" => {
    :short => "-m",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to create via create_many",
    :argument_note => "NUMBER"
  },
  "--create-individual" => {
    :short => "-i",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to create via create",
    :argument_note => "NUMBER"
  },
  "--new-individual" => {
    :short => "-I",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to create via new/save",
    :argument_note => "NUMBER"
  },
  "--update-individual" => {
    :short => "-u",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to update indivudually",
    :argument_note => "NUMBER"
  },
  "--update-many" => {
    :short => "-U",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to update via update_many",
    :argument_note => "NUMBER"
  },
}

command_line_options(@options) do |option,argument|
  if option == '--cleanup'
    $cleanup = true
  elsif option == '--force'
    $force = true
  elsif option == '--create-many'
    $create_many = argument.to_i
  elsif option == '--create-individual'
    $create_individual = argument.to_i
  elsif option == '--new-individual'
    $new_individual = argument.to_i
  elsif option == '--update-individual'
    $update_individual = argument.to_i
  elsif option == '--update-many'
    $update_many = argument.to_i
  end
end

if $cleanup || $force
  ActiveRecord::Base.connection.drop_schema("awards_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("awards") rescue nil
  ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("employees") rescue nil
  ActiveRecord::Base.connection.drop_table("companies") rescue nil
  exit(0) if $cleanup
end

$total_records = ($create_many + $create_individual + $new_individual) * 2

puts "total records: #{$total_records}"

START_DATE = Date.parse('2011-01-01')
END_DATE = Date.parse('2011-12-31')

# the ActiveRecord classes

require File.expand_path(File.dirname(__FILE__) + "/lib/company")

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
#  employees_partitions.p20101227
#  employees_partitions.p20110103
#  employees_partitions.p20110110
#  employees_partitions.p20110117
#  employees_partitions.p20110124
#  employees_partitions.p20110131
#  employees_partitions.p20110207
#  employees_partitions.p20110214
#  employees_partitions.p20110221
#  employees_partitions.p20110228
#  employees_partitions.p20110307
#  employees_partitions.p20110314
#  employees_partitions.p20110321
#  employees_partitions.p20110328
#  employees_partitions.p20110404
#  employees_partitions.p20110411
#  employees_partitions.p20110418
#  employees_partitions.p20110425
#  employees_partitions.p20110502
#  employees_partitions.p20110509
#  employees_partitions.p20110516
#  employees_partitions.p20110523
#  employees_partitions.p20110530
#  employees_partitions.p20110606
#  employees_partitions.p20110613
#  employees_partitions.p20110620
#  employees_partitions.p20110627
#  employees_partitions.p20110704
#  employees_partitions.p20110711
#  employees_partitions.p20110718
#  employees_partitions.p20110725
#  employees_partitions.p20110801
#  employees_partitions.p20110808
#  employees_partitions.p20110815
#  employees_partitions.p20110822
#  employees_partitions.p20110829
#  employees_partitions.p20110905
#  employees_partitions.p20110912
#  employees_partitions.p20110919
#  employees_partitions.p20110926
#  employees_partitions.p20111003
#  employees_partitions.p20111010
#  employees_partitions.p20111017
#  employees_partitions.p20111024
#  employees_partitions.p20111031
#  employees_partitions.p20111107
#  employees_partitions.p20111114
#  employees_partitions.p20111121
#  employees_partitions.p20111128
#  employees_partitions.p20111205
#  employees_partitions.p20111212
#  employees_partitions.p20111219
#  employees_partitions.p20111226

Award.create_new_partition_tables(dates)

# You should have the following tables with increments of one week:
#  awards_partitions.p20101227
#  awards_partitions.p20110103
#  awards_partitions.p20110110
#  awards_partitions.p20110117
#  awards_partitions.p20110124
#  awards_partitions.p20110131
#  awards_partitions.p20110207
#  awards_partitions.p20110214
#  awards_partitions.p20110221
#  awards_partitions.p20110228
#  awards_partitions.p20110307
#  awards_partitions.p20110314
#  awards_partitions.p20110321
#  awards_partitions.p20110328
#  awards_partitions.p20110404
#  awards_partitions.p20110411
#  awards_partitions.p20110418
#  awards_partitions.p20110425
#  awards_partitions.p20110502
#  awards_partitions.p20110509
#  awards_partitions.p20110516
#  awards_partitions.p20110523
#  awards_partitions.p20110530
#  awards_partitions.p20110606
#  awards_partitions.p20110613
#  awards_partitions.p20110620
#  awards_partitions.p20110627
#  awards_partitions.p20110704
#  awards_partitions.p20110711
#  awards_partitions.p20110718
#  awards_partitions.p20110725
#  awards_partitions.p20110801
#  awards_partitions.p20110808
#  awards_partitions.p20110815
#  awards_partitions.p20110822
#  awards_partitions.p20110829
#  awards_partitions.p20110905
#  awards_partitions.p20110912
#  awards_partitions.p20110919
#  awards_partitions.p20110926
#  awards_partitions.p20111003
#  awards_partitions.p20111010
#  awards_partitions.p20111017
#  awards_partitions.p20111024
#  awards_partitions.p20111031
#  awards_partitions.p20111107
#  awards_partitions.p20111114
#  awards_partitions.p20111121
#  awards_partitions.p20111128
#  awards_partitions.p20111205
#  awards_partitions.p20111212
#  awards_partitions.p20111219
#  awards_partitions.p20111226

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)

# now add some employees across the year.

employees = []

require File.expand_path(File.dirname(__FILE__) + "/lib/roman")

# generates data for employees_partitions and employees tables

base = 0
(1..$create_many).each do |i|
  employees << {
    :name => "Winston J. Sillypants, #{to_roman(base+i)}",
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
end

puts "creating many employees #{$create_many}"
Employee.create_many(employees)
base += $create_many

puts "creating individual employees #{$create_individual}"
(1..$create_individual).each do |i|
  employee_data = {
    :name => "Jonathan Crabapple, #{to_roman(base+i)}",
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
  employees << Employee.create(employee_data)
end
base += $create_individual

puts "new individual employees #{$new_individual}"
(1..$new_individual).each do |i|
  employee_data = {
    :name => "Picholine Pimplenad, #{to_roman(base+i)}",
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
  employee = Employee.new(employee_data)
  employee.save
  employees << employee
end
base += $new_individual

updates = {}
puts "update many employees#{$update_many}"
(1..$update_many).each do |i|
  index = rand(employees.length)
  employee_record = employees[index]
  updates[{
            :id => employee_record[:id],
            :created_at => employee_record[:created_at]
          }] = {
    :salary => 100
  }
end

Employee.update_many(updates, {:set_array => '"salary = #{table_name}.salary + datatable.salary, updated_at = now()"'})

puts "update individual employees #{$update_individual}"
(1..$update_individual).each do |i|
  index = rand(employees.length)
  employee_record = employees[index]
  employee = Employee.from_partition(employee_record[:created_at]).find(employee_record[:id])
  employee.salary += 1000
  employee.save
end

# generates data for awards_partitions and awards tables

awards = []

base = 0
(1..$create_many).each do |i|
  employee_record = employees[rand(employees.length)]
  awards << {
    :award_title => "You're the best #{to_roman(base+i)}",
    :awarded_on => employee_record[:created_at] + rand(1.year.seconds).seconds,
    :employee_created_at => employee_record[:created_at],
    :employee_id => employee_record[:id]
  }
end

puts "creating many awards #{$create_many}"
Award.create_many(awards)
base += $create_many

puts "creating individual awards #{$create_individual}"
(1..$create_individual).each do |i|
  employee_record = employees[rand(employees.length)]
  award_data = {
    :award_title => "You're the best #{to_roman(base+i)}",
    :awarded_on => employee_record[:created_at] + rand(1.year.seconds).seconds,
    :employee_created_at => employee_record[:created_at],
    :employee_id => employee_record[:id]
  }
  awards << Award.create(award_data)
end
base += $create_individual

puts "new individual awards #{$new_individual}"
(1..$new_individual).each do |i|
  employee_record = employees[rand(employees.length)]
  award_data = {
    :award_title => "You're the best #{to_roman(base+i)}",
    :awarded_on => employee_record[:created_at] + rand(1.year.seconds).seconds,
    :employee_created_at => employee_record[:created_at],
    :employee_id => employee_record[:id]
  }
  award = Award.new(award_data)
  award.save
  awards << award
end

base = 0
updates = {}
puts "update many awards #{$update_many}"
(1..$update_many).each do |i|
  award_record = awards[rand(awards.length)]
  updates[{
            :id => award_record[:id],
            :employee_created_at => award_record[:employee_created_at]
          }] = {
    :award_title => "You're the greatest #{to_roman(base+i)}"
  }
end

Award.update_many(updates, {:set_array => '"award_title = datatable.award_title, awarded_on = now()"'})
base += $update_many

puts "update individual #{$update_individual}"
(1..$update_individual).each do |i|
  award_record = awards[rand(awards.length)]
  award = Award.from_partition(award_record[:employee_created_at]).find(award_record[:id])
  award.awarded_on = Date.parse(Time.now.to_s)
  award.award_title = "You're the greatest #{to_roman(base+i)}"
  award.save
end
