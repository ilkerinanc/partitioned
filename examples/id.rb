#!/usr/bin/env ../../../../script/rails runner
# if you use linux, please change previous line to the
# "#!../../../../script/rails runner"
# Initial data:
#
#  Companies table is completed by four companies:
#
#  create table companies (
#        id               serial not null primary key,
#        created_at       timestamp not null default now(),
#        updated_at       timestamp,
#        name             text null
#  );
#
#  insert into companies (created_at,id,name) values
#    ('2012-03-13 13:26:52.184347',1,'Fluent Mobile, inc.'),
#    ('2012-03-13 13:26:52.184347',2,'Fiksu, inc.'),
#    ('2012-03-13 13:26:52.184347',3,'AppExchanger, inc.'),
#    ('2012-03-13 13:26:52.184347',4,'FreeMyApps, inc.');
#
#  id |         created_at         | updated_at |        name
#  ---+----------------------------+------------+---------------------
#   1 | 2012-03-11 13:26:52.184347 |            | Fluent Mobile, inc.
#   2 | 2012-03-11 13:26:52.184347 |            | Fiksu, inc.
#   3 | 2012-03-11 13:26:52.184347 |            | AppExchanger, inc.
#   4 | 2012-03-11 13:26:52.184347 |            | FreeMyApps, inc.
#
#  Employees table is associated with companies table via key - id:
#
#  create table employees (
#        id               serial not null primary key,
#        created_at       timestamp not null default now(),
#        updated_at       timestamp,
#        name             text not null,
#        salary           money not null,
#        company_id       integer not null
#  );
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
# Task:
#
#  To increase the speed of requests to the database and to reduce the time
#  of the request, need to split the Employees table to the partition tables.
#  Break criterion is an id.
#
# Implementation:
#
#  Class Employee inherits from the abstract class ById,
#  which supports partitioning.
#
#  class Employee < Partitioned::ById
#
#    Indicates a relationship to the companies table.
#    belongs_to :company, :class_name => 'Company'
#
#    Partition table size defines a count of records in one partition table.
#    def self.partition_table_size
#      return 10
#    end
#
#    Create a rules for each partition.
#    Id is a unique index. Foreign key is company_id.
#    This imposes a restriction on each of partition, that
#    the column company_id associated with the table of companies
#    and can not have values ​​that are not in the table companies.
#    In this example, set up only 4 records in the table companies,
#    so company_id can not be equal to 5 in any partition
#    until it is an established company with id = 5.
#
#    partitioned do |partition|
#       partition.foreign_key :company_id
#    end
#  end
#
#  Create a schema employees_partitions, within which to store all of our partitions:
#
#  Employee.create_infrastructure
#
#  Create a partition tables with increments of 10 records:
#
#  ids = Employee.partition_generate_range(0, NUM_EMPLOYEES, Employee.partition_table_size)
#  Employee.create_new_partition_tables(ids)
#
#  Each of partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p0" (CHECK (( id >= 0 and id < 10 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p0_id_udx" ON "employees_partitions"."p0" ("id");
#  ALTER TABLE employees_partitions.p0 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p10" (CHECK (( id >= 10 and id < 20 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p10_id_udx" ON "employees_partitions"."p10" ("id");
#  ALTER TABLE employees_partitions.p10 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20" (CHECK (( id >= 20 and id < 30 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p20_id_udx" ON "employees_partitions"."p20" ("id");
#  ALTER TABLE employees_partitions.p20 add foreign key (company_id) references companies(id);
#
#  ...
#  CREATE TABLE "employees_partitions"."p4980" (CHECK (( id >= 4980 and id < 4990 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p4980_id_udx" ON "employees_partitions"."p4980" ("id");
#  ALTER TABLE employees_partitions.p4980 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p4990" (CHECK (( id >= 4990 and id < 5000 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p4990_id_udx" ON "employees_partitions"."p4990" ("id");
#  ALTER TABLE employees_partitions.p4990 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p5000" (CHECK (( id >= 5000 and id < 5010 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p5000_id_udx" ON "employees_partitions"."p5000" ("id");
#  ALTER TABLE employees_partitions.p5000 add foreign key (company_id) references companies(id);
#
#  You should have the following tables with increments of 10 ids:
#  employees_partitions.p0
#  employees_partitions.p10
#  employees_partitions.p20
#  ...
#  employees_partitions.p4980
#  employees_partitions.p4990
#  employees_partitions.p5000
#
#  Each of partitions inherits from employees table,
#  thus a new row will automatically be added to the employees table .
#
#  To add data, we use the following construction,
#  in which employees - a random data:
#
#  Employee.create_many(employees)
#
#  The data get into the employees table ONLY through partition tables.
#  You can not do an insert row into a table employees directly.
#  For this purpose special restrictions are imposed on the table employees.
#
#  Result:
#
#  We have table companies:
#
#  id |         created_at         | updated_at |        name
#  ---+----------------------------+------------+---------------------
#   1 | 2012-03-11 13:26:52.184347 |            | Fluent Mobile, inc.
#   2 | 2012-03-11 13:26:52.184347 |            | Fiksu, inc.
#   3 | 2012-03-11 13:26:52.184347 |            | AppExchanger, inc.
#   4 | 2012-03-11 13:26:52.184347 |            | FreeMyApps, inc.
#
#  Table employees with random data from 1 to 5000:
#
#  id  |         created_at         | updated_at |                name                 |   salary    | company_id
#  ----+----------------------------+------------+-------------------------------------+-------------+------------
#    1 | 2012-03-20 13:28:38.920438 |            | Winston J. Sillypants, I            | $139,612.00 |     3
#    2 | 2012-03-20 13:28:38.920438 |            | Winston J. Sillypants, II           |  $89,303.00 |     4
#    3 | 2012-03-20 13:28:38.920438 |            | Winston J. Sillypants, III          |  $62,066.00 |     2
# ...
# 4998 | 2012-03-20 13:28:38.920438 |            | Winston J. Sillypants, MMMMCMXCVIII | $110,089.00 |     4
# 4999 | 2012-03-20 13:28:38.920438 |            | Winston J. Sillypants, MMMMCMXCIX   | $128,225.00 |     2
# 5000 | 2012-03-20 13:28:38.920438 |            | Winston J. Sillypants, _V           |  $81,125.00 |     4
#
#  Partition employees_partitions.p0 - partition where (id >= 0 AND id < 10):
#
#   id |         created_at          | updated_at |            name                     |   salary    | company_id
#  ----+-----------------------------+------------+-------------------------------------+-------------+------------
#    1 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, I            | $139,612.00 |     3
#    2 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, II           |  $89,303.00 |     4
#    3 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, III          |  $62,066.00 |     2
#    4 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, IV           |  $82,144.00 |     3
#    5 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, V            | $116,467.00 |     4
#    6 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, VI           |  $97,616.00 |     2
#    7 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, VII          | $127,854.00 |     1
#    8 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, VIII         | $112,420.00 |     1
#    9 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, IX           |  $64,514.00 |     4
#
#  Partition employees_partitions.p10 - partition where (id >= 10 AND id < 20):
#
#   id |         created_at          | updated_at |            name                     |   salary    | company_id
#  ----+-----------------------------+------------+-------------------------------------+-------------+------------
#   10 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, X            |  $96,028.00 |     3
#   11 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XI           | $123,833.00 |     2
#   12 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XII          | $113,168.00 |     3
#   13 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XIII         | $125,741.00 |     4
#   14 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XIV          | $123,324.00 |     4
#   15 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XV           |  $65,143.00 |     2
#   16 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XVI          |  $81,233.00 |     4
#   17 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XVII         | $114,756.00 |     1
#   18 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XVIII        | $106,737.00 |     3
#   19 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XIX          | $125,242.00 |     4
#
#  Partition employees_partitions.p20 - partition where (id >= 20 AND id < 30):
#
#   id |         created_at          | updated_at |            name                     |   salary    | company_id
#  ----+-----------------------------+------------+-------------------------------------+-------------+------------
#   20 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XX           | $103,387.00 |     4
#   21 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XXI          | $107,774.00 |     4
#   22 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XXII         | $122,796.00 |     3
#   23 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XXIII        |  $72,265.00 |     4
#   24 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XXIV         | $131,098.00 |     3
#   25 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XXV          | $114,342.00 |     1
#   26 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XXVI         | $136,514.00 |     2
#   27 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XXVII        |  $64,570.00 |     3
#   28 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XXVIII       |  $86,188.00 |     4
#   29 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, XXIX         |  $85,431.00 |     2
#
# ...
#  Partition employees_partitions.p4980 - partition where (id >= 4980 AND id < 4990):
#
#   id |         created_at          | updated_at |            name                       |   salary    | company_id
#  ----+-----------------------------+------------+---------------------------------------+-------------+------------
# 4980 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXX     | $100,413.00 |    3
# 4981 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXXI    |  $85,253.00 |    1
# 4982 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXXII   |  $61,951.00 |    2
# 4983 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXXIII  |  $92,285.00 |    3
# 4984 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXXIV   |  $73,148.00 |    4
# 4985 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXXV    |  $63,795.00 |    4
# 4986 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXXVI   | $125,153.00 |    2
# 4987 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXXVII  | $101,759.00 |    3
# 4988 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXXVIII | $117,156.00 |    4
# 4989 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMLXXXIX   | $103,124.00 |    4
#
#  Partition employees_partitions.p4990 - partition where (id >= 4990 AND id < 5000):
#
#   id |         created_at          | updated_at |            name                       |   salary    | company_id
#  ----+-----------------------------+------------+---------------------------------------+-------------+------------
# 4990 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXC       |  $73,148.00 |    4
# 4991 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXCI      |  $60,243.00 |    1
# 4992 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXCII     | $138,147.00 |    3
# 4993 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXCIII    | $103,401.00 |    4
# 4994 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXCIV     |  $97,833.00 |    3
# 4995 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXCV      | $113,774.00 |    1
# 4996 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXCVI     | $125,395.00 |    4
# 4997 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXCVII    |  $78,924.00 |    3
# 4998 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXCVIII   | $110,089.00 |    4
# 4999 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, MMMMCMXCIX     | $128,225.00 |    2
#
#  Partition employees_partitions.p5000 - partition where (id >= 5000 AND id < 5010):
#
#   id |         created_at          | updated_at |            name                       |   salary    | company_id
#  ----+-----------------------------+------------+---------------------------------------+-------------+------------
# 5000 | 2012-03-20 13:28:38.920438  |            | Winston J. Sillypants, _V             | $81,125.00  |    4
#

require File.expand_path(File.dirname(__FILE__) + "/lib/command_line_tool_mixin")
require File.expand_path(File.dirname(__FILE__) + "/lib/get_options")

include CommandLineToolMixin

$cleanup = false
$force = false
$create_many = 3000
$create_individual = 1000
$new_individual = 1000
$update_many = 1000
$update_individual = 1000
$partition_table_size = 10

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
  ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("employees") rescue nil
  ActiveRecord::Base.connection.drop_table("companies") rescue nil
  exit(0) if $cleanup
end

$total_records = $create_many + $create_individual + $new_individual

puts "total records: #{$total_records}"

# the ActiveRecord classes

require File.expand_path(File.dirname(__FILE__) + "/lib/company")

class Employee < Partitioned::ById
  belongs_to :company, :class_name => 'Company'

  def self.partition_table_size
    return $partition_table_size
  end

  partitioned do |partition|
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

# You should have the following tables:
#  public.companies
#  public.employees

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)

# create the infrastructure for EMPLOYEES table which includes the schema and partition tables

Employee.create_infrastructure

# You should have the following schema:
#  employees_partitions

# Create a partition tables with increments of 10 records, because
# Employee.partition_table_size returns 10
Employee.create_new_partition_tables(Range.new(0, $total_records).step(Employee.partition_table_size))

# You should have the following tables:
#  employees_partitions.p0
#  employees_partitions.p10
#  employees_partitions.p20
#  ...
#  employees_partitions.p4980
#  employees_partitions.p4990
#  employees_partitions.p5000

# now add some employees across the year.

employees = []

require File.expand_path(File.dirname(__FILE__) + "/lib/roman")

# generates data for employees_partitions and employees tables

base = 0
(1..$create_many).each do |i|
  employees << {
    :name => "Winston J. Sillypants, #{to_roman(base+i)}",
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
end

puts "creating many #{$create_many}"
Employee.create_many(employees)
base += $create_many

puts "creating individual #{$create_individual}"
(1..$create_individual).each do |i|
  employee_data = {
    :name => "Jonathan Crabapple, #{to_roman(base+i)}",
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
  employees << Employee.create(employee_data)
end
base += $create_individual

puts "new individual #{$new_individual}"
(1..$new_individual).each do |i|
  employee_data = {
    :name => "Picholine Pimplenad, #{to_roman(base+i)}",
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
  employee = Employee.new(employee_data)
  employee.save!
  employees << employee
end
base += $new_individual

updates = {}
puts "update many #{$update_many}"
(1..$update_many).each do |i|
  employee_record = employees[rand(employees.length)]
  updates[{ :id => employee_record[:id]}] = { :salary => 100 }
end

Employee.update_many(updates, {:set_array => '"salary = #{table_name}.salary + datatable.salary, updated_at = now()"'})

puts "update individual #{$update_individual}"
(1..$update_individual).each do |i|
  employee_record = employees[rand(employees.length)]
  employee = Employee.from_partition(employee_record[:id]).find(employee_record[:id])
  employee.salary += 1000
  employee.save
end
