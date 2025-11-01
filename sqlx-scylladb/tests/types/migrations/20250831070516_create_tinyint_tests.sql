-- Add migration script here
CREATE TABLE tinyint_tests(
  my_id UUID PRIMARY KEY,
  my_tinyint TINYINT,
  my_tinyint_list LIST<TINYINT>,
  my_tinyint_set SET<TINYINT>
)
