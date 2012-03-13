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
#  Break criterion is a created date(created_at).
#
# Implementation:
#
#  Class Employee inherits from the abstract class ByCreatedAt,
#  which supports partitioning.
#
#  class Employee < Partitioned::ByCreatedAt
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
#  Create a partition tables with increments of one week:
#
#  dates = Employee.partition_generate_range(START_DATE, END_DATE)
#  Employee.create_new_partition_tables(dates)
#
#  Each of partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p20101227" (CHECK (created_at >= '2010-12-27'
#                                AND created_at < '2011-01-03')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20101227_on_created_at"
#                                ON "employees_partitions"."p20101227" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20101227_on_id"
#                                ON "employees_partitions"."p20101227" ("id");
#  ALTER TABLE employees_partitions.p20101227 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20110103" (CHECK (created_at >= '2011-01-03'
#                                AND created_at < '2011-01-10')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20110103_on_created_at"
#                                ON "employees_partitions"."p20110103" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20110103_on_id"
#                                ON "employees_partitions"."p20110103" ("id");
#  ALTER TABLE employees_partitions.p20110103 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20110110" (CHECK (created_at >= '2011-01-10'
#                                AND created_at < '2011-01-17')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20110110_on_created_at"
#                                ON "employees_partitions"."p20110110" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20110110_on_id"
#                                ON "employees_partitions"."p20110110" ("id");
#  ALTER TABLE employees_partitions.p20110110 add foreign key (company_id) references companies(id);
#
#  ...
#  CREATE TABLE "employees_partitions"."p20111212" (CHECK (created_at >= '2011-12-12'
#                               AND created_at < '2011-12-19')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20111212_on_created_at"
#                               ON "employees_partitions"."p20111212" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20111212_on_id"
#                               ON "employees_partitions"."p20111212" ("id");
#  ALTER TABLE employees_partitions.p20111212 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20111219" (CHECK (created_at >= '2011-12-19'
#                               AND created_at < '2011-12-26')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20111219_on_created_at"
#                               ON "employees_partitions"."p20111219" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20111219_on_id"
#                               ON "employees_partitions"."p20111219" ("id");
#  ALTER TABLE employees_partitions.p20111219 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20111226" (CHECK (created_at >= '2011-12-26'
#                               AND created_at < '2012-01-02')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20111226_on_created_at"
#                               ON "employees_partitions"."p20111226" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20111226_on_id"
#                               ON "employees_partitions"."p20111226" ("id");
#  ALTER TABLE employees_partitions.p20111226 add foreign key (company_id) references companies(id);
#
#  You should have the following tables with increments of one week:
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
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#    1 | 2011-10-20 16:02:57 |            | Winston J. Sillypants, I            | 183.00 |     2
#    2 | 2011-06-10 14:05:28 |            | Winston J. Sillypants, II           | 145.00 |     1
#    3 | 2011-01-10 19:26:03 |            | Winston J. Sillypants, III          | 229.00 |     3
#   ...
# 4998 | 2011-02-13 23:09:57 |            | Winston J. Sillypants, MMMMCMXCVIII | 456.00 |     4
# 4999 | 2011-12-09 05:07:35 |            | Winston J. Sillypants, MMMMCMXCIX   | 751.00 |     3
# 5000 | 2011-01-26 20:45:45 |            | Winston J. Sillypants, _V           | 356.00 |     2
#
#  Partition employees_partitions.p20101227 - partition where
#  created_at >= '2010-12-27 00:00:00' AND created_at < '2011-01-03 00:00:00':
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#  142 | 2011-01-02 08:11:16 |            | Winston J. Sillypants, CXLII        | 983.00 |     4
#  263 | 2011-01-02 14:32:17 |            | Winston J. Sillypants, CCLXIII      | 848.00 |     3
#  382 | 2011-01-01 16:53:59 |            | Winston J. Sillypants, CCCLXXXII    | 307.00 |     3
#   ...
# 4594 | 2011-01-01 11:40:45 |            | Winston J. Sillypants, MMMMDXCIV    | 842.00 |     2
# 4706 | 2011-01-01 06:06:00 |            | Winston J. Sillypants, MMMMDCCVI    | 812.00 |     3
# 4728 | 2011-01-01 13:04:33 |            | Winston J. Sillypants, MMMMDCCXXVIII| 408.00 |     2
#
#  Partition employees_partitions.p20110103 - partition where
#  created_at >= '2011-01-03 00:00:00' AND created_at < '2011-01-10 00:00:00':
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#   69 | 2011-01-04 14:06:51 |            | Winston J. Sillypants, LXIX         | 211.00 |     4
#  166 | 2011-01-04 04:17:18 |            | Winston J. Sillypants, CLXVI        | 390.00 |     1
#  180 | 2011-01-09 22:28:05 |            | Winston J. Sillypants, CLXXX        | 210.00 |     4
#   ...
# 4856 | 2011-01-05 18:45:47 |            | Winston J. Sillypants, MMMMDCCCLVI  | 670.00 |     4
# 4891 | 2011-01-06 11:58:25 |            | Winston J. Sillypants, MMMMDCCCXCI  | 241.00 |     1
# 4969 | 2011-01-09 20:13:17 |            | Winston J. Sillypants, MMMMCMLXIX   | 618.00 |     1
#
#  Partition employees_partitions.p20110110 - partition where
#  created_at >= '2011-01-10 00:00:00' AND created_at < '2011-01-17 00:00:00':
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#    3 | 2011-01-10 19:26:03 |            | Winston J. Sillypants, III          | 121.00 |     2
#   36 | 2011-01-11 03:53:16 |            | Winston J. Sillypants, XXXVI        | 363.00 |     3
#  177 | 2011-01-13 16:10:29 |            | Winston J. Sillypants, CLXXVII      | 309.00 |     4
#   ...
# 4646 | 2011-01-12 21:08:13 |            | Winston J. Sillypants, MMMMDCXLVI   | 910.00 |     2
# 4950 | 2011-01-16 15:10:46 |            | Winston J. Sillypants, MMMMCML      | 731.00 |     4
# 4991 | 2011-01-11 15:48:59 |            | Winston J. Sillypants, MMMMCMXCI    | 136.00 |     3
#
# ...
#
#  Partition employees_partitions.p20111212 - partition where
#  created_at >= '2011-12-12 00:00:00' AND created_at < '2011-12-19 00:00:00':
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#   18 | 2011-12-17 02:26:23 |            | Winston J. Sillypants, XVIII        | 928.00 |     3
#  192 | 2011-12-12 13:11:14 |            | Winston J. Sillypants, CXCII        | 190.00 |     3
#  193 | 2011-12-18 22:51:16 |            | Winston J. Sillypants, CXCIII       | 135.00 |     1
#   ...
# 4830 | 2011-12-13 01:45:23 |            | Winston J. Sillypants, MMMMDCCCXXX  | 349.00 |     2
# 4844 | 2011-12-14 03:16:15 |            | Winston J. Sillypants, MMMMDCCCXLIV | 803.00 |     4
# 4890 | 2011-12-17 18:04:15 |            | Winston J. Sillypants, MMMMDCCCXC   | 038.00 |     3
#
#  Partition employees_partitions.p20111219 - partition where
#  created_at >= '2011-12-19 00:00:00' AND created_at < '2011-12-26 00:00:00':
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#   14 | 2011-12-21 00:14:37 |            | Winston J. Sillypants, XIV          | 715.00 |     3
#  110 | 2011-12-22 02:41:03 |            | Winston J. Sillypants, CX           | 432.00 |     1
#  211 | 2011-12-21 00:11:48 |            | Winston J. Sillypants, CCXI         | 333.00 |     2
#   ...
# 4731 | 2011-12-19 19:06:53 |            | Winston J. Sillypants, MMMMDCCXXXI  | 998.00 |     4
# 4821 | 2011-12-20 17:20:19 |            | Winston J. Sillypants, MMMMDCCCXXI  | 632.00 |     3
# 4992 | 2011-12-20 02:33:29 |            | Winston J. Sillypants, MMMMCMXCII   | 111.00 |     1
#
#  Partition employees_partitions.p20111226 - partition where
#  created_at >= '2011-12-26 00:00:00' AND created_at < '2012-01-02 00:00:00':
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#   89 | 2011-12-29 19:35:33 |            | Winston J. Sillypants, LXXXIX       | 709.00 |     3
#  152 | 2011-12-30 00:59:08 |            | Winston J. Sillypants, CLII         | 222.00 |     1
#  388 | 2011-12-28 02:57:46 |            | Winston J. Sillypants, CCCLXXXVIII  | 362.00 |     2
#   ...
# 4712 | 2011-12-30 11:47:08 |            | Winston J. Sillypants, MMMMDCCXII   | 846.00 |     4
# 4745 | 2011-12-28 06:18:20 |            | Winston J. Sillypants, MMMMDCCXLV   | 949.00 |     2
# 4977 | 2011-12-30 04:04:13 |            | Winston J. Sillypants, MMMMCMLXXVII | 296.00 |     1


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

class Employee < Partitioned::ByCreatedAt
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
#  employees_partitions

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

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)

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
