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
#  Break criterion is a company (company_id).
#
# Implementation:
#
#  Class Employee inherits from the abstract class ByCompanyId,
#  which supports partitioning.
#
#  class Employee < ByCompanyId
#
#    Indicates a relationship to the companies table.
#    belongs_to :company, :class_name => 'Company'
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
#      partition.index :id, :unique => true
#      partition.foreign_key :company_id
#    end
#  end
#
#  Create a schema employees_partitions, within which to store all of our partitions:
#
#  Employee.create_infrastructure
#
#  Create a partitions for each company:
#
#  company_ids = Company.all.map(&:id)
#  Employee.create_new_partition_tables(company_ids)
#
#  Each of partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p1" (CHECK (( company_id = 1 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "index_employees_partitions.p1_on_id" ON "employees_partitions"."p1" ("id");
#  ALTER TABLE employees_partitions.p1 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p2" (CHECK (( company_id = 2 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "index_employees_partitions.p2_on_id" ON "employees_partitions"."p2" ("id");
#  ALTER TABLE employees_partitions.p2 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p3" (CHECK (( company_id = 3 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "index_employees_partitions.p3_on_id" ON "employees_partitions"."p3" ("id");
#  ALTER TABLE employees_partitions.p3 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p4" (CHECK (( company_id = 4 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "index_employees_partitions.p4_on_id" ON "employees_partitions"."p4" ("id");
#  ALTER TABLE employees_partitions.p4 add foreign key (company_id) references companies(id);
#
#  Since we have done four records of companies in the table,
#  we have four partitions:
#
#  employees_partitions.p1
#  employees_partitions.p2
#  employees_partitions.p3
#  employees_partitions.p4
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
#   id |         created_at         | updated_at |            name                     | salary | company_id
#  ----+----------------------------+------------+-------------------------------------+--------+------------
#    1 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, I            | 183.00 |     2
#    2 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, II           | 145.00 |     1
#    3 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, III          | 229.00 |     3
#   ...
# 4998 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMXCVIII | 456.00 |     4
# 4999 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMXCIX   | 751.00 |     3
# 5000 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, _V           | 356.00 |     2
#
#  Partition employees_partitions.p1 - partition where company_id = 1:
#
#   id |         created_at         | updated_at |            name                     | salary | company_id
#  ----+----------------------------+------------+-------------------------------------+--------+------------
#    2 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, II           | 145.00 |     1
#    8 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, XIII         | 307.00 |     1
#   12 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, XVII         | 812.00 |     1
#   ...
# 4986 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMLXXXVI | 65.00  |     1
# 4995 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMXCV    | 316.00 |     1
# 4997 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMXCVII  | 119.00 |     1
#
#  Partition employees_partitions.p2 - partition where company_id = 2:
#
#   id |         created_at         | updated_at |            name                     | salary | company_id
#  ----+----------------------------+------------+-------------------------------------+--------+------------
#    1 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, I            | 183.00 |     2
#    9 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, IX           | 840.00 |     2
#   11 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, XI           | 943.00 |     2
#   ...
# 4994 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMXCIV   | 712.00 |     2
# 4996 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMXCVI   | 127.00 |     2
# 5000 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, _V           | 356.00 |     2
#
#  Partition employees_partitions.p3 - partition where company_id = 3:
#
#   id |         created_at         | updated_at |            name                     | salary | company_id
#  ----+----------------------------+------------+-------------------------------------+--------+------------
#    3 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, III          |  229.00 |    3
#    4 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, IV           |  475.00 |    3
#    6 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, VI           |  997.00 |    3
#   ...
# 4974 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMLXXIV  |  405.00 |    3
# 4982 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMLXXXII |  497.00 |    3
# 4999 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMXCIX   |  751.00 |    3
#
#  Partition employees_partitions.p4 - partition where company_id = 4:
#
#   id |         created_at         | updated_at |            name                     | salary | company_id
#  ----+----------------------------+------------+-------------------------------------+--------+------------
#    5 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, V            | 609.00 |     4
#    7 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, VII          | 348.00 |     4
#   10 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, X            | 744.00 |     4
#   ...
# 4989 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMLXXXIX | 224.00 |     4
# 4991 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMXCI    | 728.00 |     4
# 4998 | 2012-03-11 13:26:52.811381 |            | Winston J. Sillypants, MMMMCMXCVIII | 456.00 |     4
#

if ['--cleanup', '--force'].include?(ARGV[0])
  ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("employees") rescue nil
  ActiveRecord::Base.connection.drop_table("companies") rescue nil
  exit(0) if ARGV[0] == '--cleanup'
end

NUM_EMPLOYEES = 5000

# the ActiveRecord classes

require File.expand_path(File.dirname(__FILE__) + "/lib/company")
require File.expand_path(File.dirname(__FILE__) + "/lib/by_company_id")

class Employee < ByCompanyId
  belongs_to :company, :class_name => 'Company'

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

# create the infrastructure for EMPLOYEES table which includes the schema and partition tables

Employee.create_infrastructure

# You should have the following schema:
# employees_partitions

# add some companies

Company.create_many(COMPANIES)

# create the employees partitions dependant on the all companies

company_ids = Company.all.map(&:id)
Employee.create_new_partition_tables(company_ids)

# You should have the following tables:
#  employees_partitions.p1
#  employees_partitions.p2
#  employees_partitions.p3
#  employees_partitions.p4

employees = []

require File.expand_path(File.dirname(__FILE__) + "/lib/roman")

# generates data for employees_partitions and employees tables

(1..NUM_EMPLOYEES).each do |i|
  employees << {
    :name => "Winston J. Sillypants, #{to_roman(i)}",
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
end

Employee.create_many(employees)
