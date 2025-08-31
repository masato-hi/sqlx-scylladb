-- Add migration script here
CREATE TABLE double_tests(
  my_id UUID PRIMARY KEY,
  my_double DOUBLE,
  my_double_list LIST<DOUBLE>,
  my_double_set SET<DOUBLE>
)
