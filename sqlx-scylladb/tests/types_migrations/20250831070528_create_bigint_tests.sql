-- Add migration script here
CREATE TABLE bigint_tests(
  my_id UUID PRIMARY KEY,
  my_bigint BIGINT,
  my_bigint_list LIST<BIGINT>,
  my_bigint_set SET<BIGINT>
)
