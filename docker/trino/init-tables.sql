CREATE SCHEMA IF NOT EXISTS delta.analytics
WITH (location = 's3://gold/');

USE delta.analytics;

CALL delta.system.register_table(
  schema_name => 'analytics',
  table_name  => 'user_dim',
  table_location => 's3://gold/user_dim'
);

CALL delta.system.register_table(
  schema_name => 'analytics',
  table_name  => 'location_dim',
  table_location => 's3://gold/location_dim'
);

CALL delta.system.register_table(
  schema_name => 'analytics',
  table_name  => 'date_dim',
  table_location => 's3://gold/date_dim'
);

SHOW TABLES;