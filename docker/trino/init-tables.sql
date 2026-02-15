CREATE SCHEMA IF NOT EXISTS delta.analytics
WITH (location = 's3://gold/');

USE delta.analytics;

CALL delta.system.register_table(
  schema_name => 'analytics',
  table_name  => 'user_dim',
  table_location => 's3://gold/gold_user_dim'
);

CALL delta.system.register_table(
  schema_name => 'analytics',
  table_name  => 'location_dim',
  table_location => 's3://gold/gold_location_dim'
);

CALL delta.system.register_table(
  schema_name => 'analytics',
  table_name  => 'date_dim',
  table_location => 's3://gold/gold_date_dim'
);

CALL delta.system.register_table(
  schema_name => 'analytics',
  table_name  => 'product_dim',
  table_location => 's3://gold/gold_product_dim'
);

CALL delta.system.register_table(
  schema_name => 'analytics',
  table_name  => 'sales_fact',
  table_location => 's3://gold/gold_sales_fact'
);
SHOW TABLES;