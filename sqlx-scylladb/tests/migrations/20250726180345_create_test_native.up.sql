-- Add up migration script here
CREATE TABLE test_native(
  my_test_id UUID PRIMARY KEY,
  my_boolean BOOLEAN,
  my_tinyint TINYINT,
  my_smallint SMALLINT,
  my_int INT,
  my_bigint BIGINT,
  my_float FLOAT,
  my_double DOUBLE,
  my_ascii ASCII,
  my_text TEXT,
  my_blob BLOB,
  my_inet INET,
  my_uuid UUID,
  my_timeuuid TIMEUUID,
  my_date DATE,
  my_time TIME,
  my_timestamp TIMESTAMP,
  my_decimal DECIMAL,
  -- my_varint VARINT, -- not supported
);
