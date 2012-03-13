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
#  Break criterion are company(company_id) and created date(created_at).
#
# Implementation:
#
#  Class Employee inherits from the abstract class MultiLevel,
#  which supports multilevel partitioning.
#
#  class Employee < Partitioned::MultiLevel
#
#    Indicates a relationship to the companies table.
#    belongs_to :company, :class_name => 'Company'
#
#    Create a rules for each partition.
#    Will be created on the partition for each company,
#    And will be created on the partition for each company in increments of one week.
#
#    partitioned do |partition|
#      partition.using_classes ByCompanyId, Partitioned::ByCreatedAt
#    end
#  end
#
#  Create a schema employees_partitions, within which to store all of our partitions:
#
#  Employee.create_infrastructure
#
#  Create a partition for each of company and each of company across the week:
#
#  Where partition_key_values is array of pairs company_id and date.
#  Employee.create_new_partition_tables(partition_key_values)
#
#  Each of partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p1" (CHECK (( company_id = 1 ))) INHERITS (employees);
#  CREATE TABLE "employees_partitions"."p1_20101227" (CHECK (created_at >= '2010-12-27'
#                    AND created_at < '2011-01-03')) INHERITS (employees_partitions.p1);
#  CREATE TABLE "employees_partitions"."p1_20110103" (CHECK (created_at >= '2011-01-03'
#                    AND created_at < '2011-01-10')) INHERITS (employees_partitions.p1);
#  CREATE TABLE "employees_partitions"."p1_20110110" (CHECK (created_at >= '2011-01-10'
#                    AND created_at < '2011-01-17')) INHERITS (employees_partitions.p1);
#  ...
#  CREATE TABLE "employees_partitions"."p1_20111212" (CHECK (created_at >= '2011-12-12'
#                   AND created_at < '2011-12-19')) INHERITS (employees_partitions.p1);
#  CREATE TABLE "employees_partitions"."p1_20111219" (CHECK (created_at >= '2011-12-19'
#                   AND created_at < '2011-12-26')) INHERITS (employees_partitions.p1);
#  CREATE TABLE "employees_partitions"."p1_20111226" (CHECK (created_at >= '2011-12-26'
#                   AND created_at < '2012-01-02')) INHERITS (employees_partitions.p1);
#
#  CREATE TABLE "employees_partitions"."p2" (CHECK (( company_id = 2 ))) INHERITS (employees);
#  CREATE TABLE "employees_partitions"."p2_20101227" (CHECK (created_at >= '2010-12-27'
#                   AND created_at < '2011-01-03')) INHERITS (employees_partitions.p2);
#  CREATE TABLE "employees_partitions"."p2_20110103" (CHECK (created_at >= '2011-01-03'
#                   AND created_at < '2011-01-10')) INHERITS (employees_partitions.p2);
#  CREATE TABLE "employees_partitions"."p2_20110110" (CHECK (created_at >= '2011-01-10'
#                   AND created_at < '2011-01-17')) INHERITS (employees_partitions.p2);
#  ...
#  CREATE TABLE "employees_partitions"."p2_20111212" (CHECK (created_at >= '2011-12-12'
#                   AND created_at < '2011-12-19')) INHERITS (employees_partitions.p2);
#  CREATE TABLE "employees_partitions"."p2_20111219" (CHECK (created_at >= '2011-12-19'
#                   AND created_at < '2011-12-26')) INHERITS (employees_partitions.p2);
#  CREATE TABLE "employees_partitions"."p2_20111226" (CHECK (created_at >= '2011-12-26'
#                   AND created_at < '2012-01-02')) INHERITS (employees_partitions.p2);
#
#  CREATE TABLE "employees_partitions"."p3" (CHECK (( company_id = 3 ))) INHERITS (employees);
#  CREATE TABLE "employees_partitions"."p3_20101227" (CHECK (created_at >= '2010-12-27'
#                   AND created_at < '2011-01-03')) INHERITS (employees_partitions.p3);
#  CREATE TABLE "employees_partitions"."p3_20110103" (CHECK (created_at >= '2011-01-03'
#                   AND created_at < '2011-01-10')) INHERITS (employees_partitions.p3);
#  CREATE TABLE "employees_partitions"."p3_20110110" (CHECK (created_at >= '2011-01-10'
#                   AND created_at < '2011-01-17')) INHERITS (employees_partitions.p3);
#  ...
#  CREATE TABLE "employees_partitions"."p3_20111212" (CHECK (created_at >= '2011-12-12'
#                   AND created_at < '2011-12-19')) INHERITS (employees_partitions.p3);
#  CREATE TABLE "employees_partitions"."p3_20111219" (CHECK (created_at >= '2011-12-19'
#                   AND created_at < '2011-12-26')) INHERITS (employees_partitions.p3);
#  CREATE TABLE "employees_partitions"."p3_20111226" (CHECK (created_at >= '2011-12-26'
#                   AND created_at < '2012-01-02')) INHERITS (employees_partitions.p3);
#
#  CREATE TABLE "employees_partitions"."p4" (CHECK (( company_id = 4 ))) INHERITS (employees);
#  CREATE TABLE "employees_partitions"."p4_20101227" (CHECK (created_at >= '2010-12-27'
#                   AND created_at < '2011-01-03')) INHERITS (employees_partitions.p4);
#  CREATE TABLE "employees_partitions"."p4_20110103" (CHECK (created_at >= '2011-01-03'
#                   AND created_at < '2011-01-10')) INHERITS (employees_partitions.p4);
#  CREATE TABLE "employees_partitions"."p4_20110110" (CHECK (created_at >= '2011-01-10'
#                   AND created_at < '2011-01-17')) INHERITS (employees_partitions.p4);
#  ...
#  CREATE TABLE "employees_partitions"."p4_20111212" (CHECK (created_at >= '2011-12-12'
#                   AND created_at < '2011-12-19')) INHERITS (employees_partitions.p4);
#  CREATE TABLE "employees_partitions"."p4_20111219" (CHECK (created_at >= '2011-12-19'
#                   AND created_at < '2011-12-26')) INHERITS (employees_partitions.p4);
#  CREATE TABLE "employees_partitions"."p4_20111226" (CHECK (created_at >= '2011-12-26'
#                   AND created_at < '2012-01-02')) INHERITS (employees_partitions.p4);
#
#  You should have the following tables:
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
#    1 | 2011-01-13 14:53:58 |            | Winston J. Sillypants, I            | 183.00 |     2
#    2 | 2011-03-17 21:18:15 |            | Winston J. Sillypants, II           | 145.00 |     1
#    3 | 2011-09-24 23:10:09 |            | Winston J. Sillypants, III          | 229.00 |     3
#   ...
# 4998 | 2011-07-29 16:33:10 |            | Winston J. Sillypants, MMMMCMXCVIII | 456.00 |     4
# 4999 | 2011-08-05 11:51:12 |            | Winston J. Sillypants, MMMMCMXCIX   | 751.00 |     3
# 5000 | 2011-07-23 22:57:44 |            | Winston J. Sillypants, _V           | 356.00 |     2
#
#  Partition employees_partitions.p1 - partition where company_id = 1:
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#    2 | 2011-03-17 21:18:15 |            | Winston J. Sillypants, II           | 145.00 |     1
#    8 | 2011-08-19 17:45:18 |            | Winston J. Sillypants, XIII         | 307.00 |     1
#   12 | 2011-11-01 09:12:33 |            | Winston J. Sillypants, XVII         | 812.00 |     1
#   ...
# 4986 | 2011-06-21 20:17:05 |            | Winston J. Sillypants, MMMMCMLXXXVI | 65.00  |     1
# 4995 | 2011-07-20 10:08:25 |            | Winston J. Sillypants, MMMMCMXCV    | 316.00 |     1
# 4997 | 2011-06-13 19:48:31 |            | Winston J. Sillypants, MMMMCMXCVII  | 119.00 |     1
#
#  Partition employees_partitions.p2 - partition where company_id = 2:
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#    1 | 2011-01-13 14:53:58 |            | Winston J. Sillypants, I            | 183.00 |     2
#    9 | 2011-05-14 14:45:35 |            | Winston J. Sillypants, IX           | 840.00 |     2
#   11 | 2011-09-09 18:25:51 |            | Winston J. Sillypants, XI           | 943.00 |     2
#   ...
# 4994 | 2011-11-19 20:01:08 |            | Winston J. Sillypants, MMMMCMXCIV   | 712.00 |     2
# 4996 | 2011-12-01 04:49:53 |            | Winston J. Sillypants, MMMMCMXCVI   | 127.00 |     2
# 5000 | 2011-07-23 22:57:44 |            | Winston J. Sillypants, _V           | 356.00 |     2
#
#  Partition employees_partitions.p3 - partition where company_id = 3:
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#    3 | 2011-09-24 23:10:09 |            | Winston J. Sillypants, III          |  229.00 |    3
#    4 | 2011-09-30 04:40:19 |            | Winston J. Sillypants, IV           |  475.00 |    3
#    6 | 2011-12-22 09:13:19 |            | Winston J. Sillypants, VI           |  997.00 |    3
#   ...
# 4974 | 2011-06-21 13:19:23 |            | Winston J. Sillypants, MMMMCMLXXIV  |  405.00 |    3
# 4982 | 2011-04-30 08:37:28 |            | Winston J. Sillypants, MMMMCMLXXXII |  497.00 |    3
# 4999 | 2011-08-05 11:51:12 |            | Winston J. Sillypants, MMMMCMXCIX   |  751.00 |    3
#
#  Partition employees_partitions.p4 - partition where company_id = 4:
#
#   id |         created_at  | updated_at |            name                     | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#    5 | 2011-12-18 01:45:17 |            | Winston J. Sillypants, V            | 609.00 |     4
#    7 | 2011-03-14 13:52:51 |            | Winston J. Sillypants, VII          | 348.00 |     4
#   10 | 2011-10-31 14:57:53 |            | Winston J. Sillypants, X            | 744.00 |     4
#   ...
# 4989 | 2011-10-19 15:24:52 |            | Winston J. Sillypants, MMMMCMLXXXIX | 224.00 |     4
# 4991 | 2011-04-07 11:49:35 |            | Winston J. Sillypants, MMMMCMXCI    | 728.00 |     4
# 4998 | 2011-07-29 16:33:10 |            | Winston J. Sillypants, MMMMCMXCVIII | 456.00 |     4
#
#  Partition employees_partitions.p1_20101227 - partition where company_id = 1
#  and created_at >= '2010-12-27 00:00:00' AND created_at < '2011-01-03 00:00:00':
#
#  id  |     created_at      | updated_at |                name                 | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#   93 | 2011-01-01 11:33:09 |            | Winston J. Sillypants, XCIII        | 930.00 |     1
#  801 | 2011-01-02 05:52:54 |            | Winston J. Sillypants, DCCCI        | 635.00 |     1
# 1723 | 2011-01-02 07:14:59 |            | Winston J. Sillypants, MDCCXXIII    | 643.00 |     1
# 1887 | 2011-01-02 19:10:22 |            | Winston J. Sillypants, MDCCCLXXXVII | 078.00 |     1
# 1936 | 2011-01-02 00:35:01 |            | Winston J. Sillypants, MCMXXXVI     | 725.00 |     1
# 2344 | 2011-01-01 01:24:50 |            | Winston J. Sillypants, MMCCCXLIV    | 898.00 |     1
# 4257 | 2011-01-01 13:57:21 |            | Winston J. Sillypants, MMMMCCLVII   | 602.00 |     1
# 4441 | 2011-01-01 09:08:22 |            | Winston J. Sillypants, MMMMCDXLI    | 438.00 |     1
#
#  Partition employees_partitions.p1_20110103 - partition where company_id = 1
#  and created_at >= '2011-01-03 00:00:00' AND created_at < '2011-01-10 00:00:00':
#
#  id  |     created_at      | updated_at |                name                 | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#   23 | 2011-01-09 13:17:28 |            | Winston J. Sillypants, XXIII        | 026.00 |     1
#   40 | 2011-01-09 04:17:09 |            | Winston J. Sillypants, XL           | 367.00 |     1
#   58 | 2011-01-06 02:49:27 |            | Winston J. Sillypants, LVIII        | 779.00 |     1
#  177 | 2011-01-07 13:31:26 |            | Winston J. Sillypants, CLXXVII      | 043.00 |     1
#  417 | 2011-01-07 03:53:09 |            | Winston J. Sillypants, CDXVII       | 545.00 |     1
#  953 | 2011-01-06 17:27:19 |            | Winston J. Sillypants, CMLIII       | 633.00 |     1
# 1016 | 2011-01-04 12:25:42 |            | Winston J. Sillypants, MXVI         | 946.00 |     1
# 1079 | 2011-01-05 06:16:34 |            | Winston J. Sillypants, MLXXIX       | 367.00 |     1
# 1124 | 2011-01-06 06:31:07 |            | Winston J. Sillypants, MCXXIV       | 863.00 |     1
# 1166 | 2011-01-09 10:43:00 |            | Winston J. Sillypants, MCLXVI       | 880.00 |     1
# 1701 | 2011-01-04 16:26:03 |            | Winston J. Sillypants, MDCCI        | 352.00 |     1
# 2418 | 2011-01-08 17:40:34 |            | Winston J. Sillypants, MMCDXVIII    | 281.00 |     1
# 3379 | 2011-01-06 13:25:54 |            | Winston J. Sillypants, MMMCCCLXXIX  | 479.00 |     1
# 3662 | 2011-01-06 23:57:48 |            | Winston J. Sillypants, MMMDCLXII    | 479.00 |     1
# 3734 | 2011-01-08 01:44:44 |            | Winston J. Sillypants, MMMDCCXXXIV  | 817.00 |     1
# 4094 | 2011-01-03 22:24:40 |            | Winston J. Sillypants, MMMMXCIV     | 344.00 |     1
# 4286 | 2011-01-09 20:02:02 |            | Winston J. Sillypants, MMMMCCLXXXVI | 635.00 |     1
# 4515 | 2011-01-06 20:12:34 |            | Winston J. Sillypants, MMMMDXV      | 313.00 |     1
# 4610 | 2011-01-03 04:20:26 |            | Winston J. Sillypants, MMMMDCX      | 247.00 |     1
# 4700 | 2011-01-08 09:57:29 |            | Winston J. Sillypants, MMMMDCC      | 506.00 |     1
# 4785 | 2011-01-09 01:03:07 |            | Winston J. Sillypants, MMMMDCCLXXXV | 285.00 |     1
# 4840 | 2011-01-03 08:29:13 |            | Winston J. Sillypants, MMMMDCCCXL   | 144.00 |     1
# 4953 | 2011-01-04 01:06:05 |            | Winston J. Sillypants, MMMMCMLIII   | 968.00 |     1
#
# ...
#
#  Partition employees_partitions.p1_20111219 - partition where company_id = 1
#  and created_at >= '2011-12-19 00:00:00' AND created_at < '2011-12-26 00:00:00':
#
#  id  |     created_at      | updated_at |                name                 | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#   36 | 2011-12-24 06:28:27 |            | Winston J. Sillypants, V            | 801.00 |     1
#  150 | 2011-12-23 16:25:02 |            | Winston J. Sillypants, CL           | 970.00 |     1
#  251 | 2011-12-22 08:29:16 |            | Winston J. Sillypants, CCLI         | 536.00 |     1
#  418 | 2011-12-22 18:34:40 |            | Winston J. Sillypants, CDXVIII      | 987.00 |     1
#  577 | 2011-12-24 21:02:58 |            | Winston J. Sillypants, DLXXVII      | 357.00 |     1
#  844 | 2011-12-25 23:55:35 |            | Winston J. Sillypants, DCCCXLIV     | 693.00 |     1
#  933 | 2011-12-24 20:41:32 |            | Winston J. Sillypants, CMXXXIII     | 612.00 |     1
# 1219 | 2011-12-20 00:16:57 |            | Winston J. Sillypants, MCCXIX       | 747.00 |     1
# 1247 | 2011-12-22 18:14:38 |            | Winston J. Sillypants, MCCXLVII     | 582.00 |     1
# 1485 | 2011-12-19 08:11:14 |            | Winston J. Sillypants, MCDLXXXV     | 519.00 |     1
# 1519 | 2011-12-21 05:52:10 |            | Winston J. Sillypants, MDXIX        | 450.00 |     1
# 1597 | 2011-12-21 20:24:20 |            | Winston J. Sillypants, MDXCVII      | 746.00 |     1
# 1694 | 2011-12-21 05:25:36 |            | Winston J. Sillypants, MDCXCIV      | 876.00 |     1
# 1826 | 2011-12-22 06:46:45 |            | Winston J. Sillypants, MDCCCXXVI    | 501.00 |     1
# 2116 | 2011-12-21 00:46:32 |            | Winston J. Sillypants, MMCXVI       | 650.00 |     1
# 2414 | 2011-12-23 18:18:30 |            | Winston J. Sillypants, MMCDXIV      | 573.00 |     1
# 2666 | 2011-12-25 23:02:01 |            | Winston J. Sillypants, MMDCLXVI     | 134.00 |     1
# 2796 | 2011-12-25 09:21:57 |            | Winston J. Sillypants, MMDCCXCVI    | 389.00 |     1
# 2880 | 2011-12-20 05:13:04 |            | Winston J. Sillypants, MMDCCCLXXX   | 930.00 |     1
# 2941 | 2011-12-19 00:11:42 |            | Winston J. Sillypants, MMCMXLI      | 599.00 |     1
# 3044 | 2011-12-19 13:12:24 |            | Winston J. Sillypants, MMMXLIV      | 896.00 |     1
# 3215 | 2011-12-21 08:16:24 |            | Winston J. Sillypants, MMMCCXV      | 896.00 |     1
# 4679 | 2011-12-24 18:09:53 |            | Winston J. Sillypants, MMMMDCLXXIX  | 102.00 |     1
# 4872 | 2011-12-25 13:07:57 |            | Winston J. Sillypants, MMMMDCCCLXXII| 581.00 |     1
#
#  Partition employees_partitions.p1_20111226 - partition where company_id = 1
#  and created_at >= '2011-12-26 00:00:00' AND created_at < '2012-01-02 00:00:00':
#
#  id  |     created_at      | updated_at |                name                 | salary | company_id
#  ----+---------------------+------------+-------------------------------------+--------+------------
#  107 | 2011-12-27 01:13:10 |            | Winston J. Sillypants, CVII         | 319.00 |     1
#  194 | 2011-12-27 15:35:01 |            | Winston J. Sillypants, CXCIV        | 520.00 |     1
# 1464 | 2011-12-27 07:01:27 |            | Winston J. Sillypants, MCDLXIV      | 538.00 |     1
# 1648 | 2011-12-26 19:45:44 |            | Winston J. Sillypants, MDCXLVIII    | 869.00 |     1
# 2008 | 2011-12-28 02:19:17 |            | Winston J. Sillypants, MMVIII       | 876.00 |     1
# 2410 | 2011-12-28 15:20:33 |            | Winston J. Sillypants, MMCDX        | 280.00 |     1
# 2686 | 2011-12-26 22:30:12 |            | Winston J. Sillypants, MMDCLXXXVI   | 185.00 |     1
# 2719 | 2011-12-26 12:16:50 |            | Winston J. Sillypants, MMDCCXIX     | 717.00 |     1
# 3118 | 2011-12-26 04:12:57 |            | Winston J. Sillypants, MMMCXVIII    | 911.00 |     1
# 3296 | 2011-12-27 23:25:19 |            | Winston J. Sillypants, MMMCCXCVI    | 995.00 |     1
# 3323 | 2011-12-28 15:56:50 |            | Winston J. Sillypants, MMMCCCXXIII  | 409.00 |     1
# 3668 | 2011-12-26 01:48:33 |            | Winston J. Sillypants, MMMDCLXVIII  | 742.00 |     1
# 3709 | 2011-12-26 09:14:16 |            | Winston J. Sillypants, MMMDCCIX     | 265.00 |     1
# 4435 | 2011-12-28 02:21:10 |            | Winston J. Sillypants, MMMMCDXXXV   | 111.00 |     1
# 4728 | 2011-12-27 15:38:50 |            | Winston J. Sillypants, MMMMDCCXXVIII| 502.00 |     1
#
# A similar behavior for all other partitions where company_id = 2, 3, 4.

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

# create the infrastructure for EMPLOYEES table which includes the schema
Employee.create_infrastructure

# You should have the following schema:
#  employees_partitions

# create the employees partition tables
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
