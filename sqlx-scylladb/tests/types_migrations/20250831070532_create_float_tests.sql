-- Add migration script here
CREATE TABLE float_tests(
  my_id UUID PRIMARY KEY,
  my_float FLOAT,
  my_float_list LIST<FLOAT>,
  my_float_set SET<FLOAT>
)
