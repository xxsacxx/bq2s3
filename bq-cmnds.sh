###to create partition wrt ingested time for query result:

bq query --use_legacy_sql=false \
 --destination_table 'karmalife-683bd:Superset.temps$20200330' \
 --time_partitioning_type=DAY \
'SELECT
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
  TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch,
  MAX(
  IF
    (param.key = "Action",
      param.value.string_value,
      NULL)) AS main_event,
  MAX(
  IF
    (param.key = "Geo_Coordinates",
      param.value.string_value,
      NULL)) AS location_coordinates,
  MAX(
  IF
    (param.key = "Location",
      param.value.string_value,
      NULL)) AS Location,
  MAX(
  IF
    (param.key = "Label",
      param.value.string_value,
      NULL)) AS Label,
  MAX(
  IF
    (param.key = "Network",
      param.value.string_value,
      NULL)) AS Network,
  MAX(
  IF
    (param.key = "UserId",
      param.value.string_value,
      NULL)) AS UserId,
  MAX(
  IF
    (param.key = "firebase_event_origin",
      param.value.string_value,
      NULL)) AS firebase_event_origin,
  MAX(
  IF
    (param.key = "Category",
      param.value.string_value,
      NULL)) AS Category
FROM (
  SELECT
    user_pseudo_id,
    event_timestamp,
    user_first_touch_timestamp,
    param
  FROM
    `karmalife-683bd.analytics_206858673.events_20200330`,
    UNNEST(event_params) AS param
  WHERE
    event_name = "KL_EVENT"
    AND (param.key = "Action"
      OR param.key = "Geo_Coordinates"
      OR param.key = "Location"
      OR param.key = "Label"
      OR param.key = "Network"
      OR param.key = "UserId"
      OR param.key = "firebase_event_origin"
      OR param.key = "Category") )
GROUP BY
  user_pseudo_id,
  event_timestamp,
  user_first_touch_timestamp'