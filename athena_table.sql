CREATE EXTERNAL TABLE bq_data_testing_prtn(
  event_timestamp double,
  user_first_touch double,
  main_event string,
  location_coordinates string,
  locaton string,
  network string,
  userid string,
  firebase_event_origin string,
  category string)
PARTITIONED BY (year STRING,month STRING,day STRING)
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.literal'='
{
  "type" : "record",
  "name" : "Root",
  "fields" : [ {
    "name" : "event_timestamp",
    "type" : [ "null", {
      "type" : "long",
      "logicalType" : "timestamp-micros"
    } ]
  }, {
    "name" : "user_first_touch",
    "type" : [ "null", {
      "type" : "long",
      "logicalType" : "timestamp-micros"
    } ]
  }, {
    "name" : "main_event",
    "type" : [ "null", "string" ]
  }, {
    "name" : "location_coordinates",
    "type" : [ "null", "string" ]
  }, {
    "name" : "Location",
    "type" : [ "null", "string" ]
  }, {
    "name" : "Label",
    "type" : [ "null", "string" ]
  }, {
    "name" : "Network",
    "type" : [ "null", "string" ]
  }, {
    "name" : "UserId",
    "type" : [ "null", "string" ]
  }, {
    "name" : "firebase_event_origin",
    "type" : [ "null", "string" ]
  }, {
    "name" : "Category",
    "type" : [ "null", "string" ]
  } ]
}
')
STORED AS AVRO
LOCATION 's3://<bucket>';