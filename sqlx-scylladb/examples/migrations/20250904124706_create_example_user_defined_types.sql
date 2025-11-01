-- Add migration script here
CREATE TABLE IF NOT EXISTS example_user_defined_types(
  id BIGINT PRIMARY KEY,
  my_udt example_user_defined_type,
  my_udt_list LIST<FROZEN<example_user_defined_type>>
)
