-- Add migration script here
CREATE TABLE smallint_tests(
  my_id UUID PRIMARY KEY,
  my_smallint SMALLINT,
  my_smallint_list LIST<SMALLINT>,
  my_smallint_set SET<SMALLINT>
)
