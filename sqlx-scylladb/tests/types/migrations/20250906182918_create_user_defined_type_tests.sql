-- Add migration script here
CREATE TABLE user_defined_type_tests(
  my_id UUID PRIMARY KEY,
  my_user_defined_type my_user_defined_type,
  my_user_defined_type_list LIST<FROZEN<my_user_defined_type>>,
  my_user_defined_type_set SET<FROZEN<my_user_defined_type>>
)
