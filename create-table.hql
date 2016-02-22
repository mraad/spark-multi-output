drop table if exists trips;
create external table if not exists trips (
pickupdatetime string,
dropoffdatetime string,
pickupx double,
pickupy double,
dropoffx double,
dropoffy double,
passengercount int,
triptime int,
tripdist double,
rc25 string,
rc50 string,
rc100 string,
rc200 string
) partitioned by (year int, month int, day int, hour int)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile;
