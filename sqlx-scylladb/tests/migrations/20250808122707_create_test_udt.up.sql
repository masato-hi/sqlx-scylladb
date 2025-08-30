-- Add up migration script here
CREATE TABLE test_udt(
  my_test_id UUID PRIMARY KEY,
  my_udt my_udt,
  my_udt_set SET<FROZEN<my_udt>>
);
