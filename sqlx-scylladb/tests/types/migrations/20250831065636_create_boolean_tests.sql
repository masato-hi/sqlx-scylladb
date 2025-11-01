-- Add migration script here
CREATE TABLE boolean_tests(
  my_id UUID PRIMARY KEY,
  my_boolean BOOLEAN,
  my_boolean_list LIST<BOOLEAN>,
  my_boolean_set SET<BOOLEAN>
)
