-- Add migration script here
CREATE TABLE ascii_tests(
  my_id UUID PRIMARY KEY,
  my_ascii ASCII,
  my_ascii_list LIST<ASCII>,
  my_ascii_set SET<ASCII>
)
