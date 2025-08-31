-- Add migration script here
CREATE TABLE int_tests(
  my_id UUID PRIMARY KEY,
  my_int INT,
  my_int_list LIST<INT>,
  my_int_set SET<INT>
)
