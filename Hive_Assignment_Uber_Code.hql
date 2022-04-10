#----Creating a database
create database hive_uber;

#----Switching to the database
use hive_uber;

#----Createa a table with the same structure of out input file
create table uber(base string,travel_date string,vehicles int,trips int)row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1");

#----Insert records into the table from the file
load data local inpath '/home/hadoop/Learnbay/uber' into table uber;

#----Perform the use case 1 by writing the query and storing the results into a local file
insert overwrite local directory '/home/hadoop/Learnbay/uber_usecase1' select conv_date,sum(vehicles) from (select dayofmonth(to_date(from_unixtime(UNIX_TIMESTAMP(travel_date,'MM/dd/yyyy')))) as conv_date,vehicles from uber) A group by conv_date;

#----Perform the use case 2 by writing the query and storing the results into a local file
insert overwrite local directory '/home/hadoop/Learnbay/uber_usecase2' select base,MAX(trips) from uber group by base;
