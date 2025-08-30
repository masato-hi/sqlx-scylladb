-- Add up migration script here
CREATE TABLE test_collection(
  my_test_id UUID PRIMARY KEY,
  my_boolean_array SET<BOOLEAN>,
  my_list LIST<BIGINT>,
  my_set SET<BIGINT>,
  my_map MAP<TEXT, BIGINT>,
  my_tuple TUPLE<BIGINT, TEXT, INT>,
);
