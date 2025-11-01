-- Add migration script here
CREATE TABLE tuple_tests(
  my_id UUID PRIMARY KEY,
  my_tuple TUPLE<BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, UUID, TIMESTAMP, DATE, TIME, ASCII, TEXT, INET, BLOB, my_user_defined_type>
)
