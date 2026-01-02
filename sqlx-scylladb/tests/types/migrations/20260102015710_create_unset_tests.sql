-- Add migration script here
CREATE TABLE unset_tests(
  my_id UUID PRIMARY KEY,
  my_text TEXT,
  my_bigint BIGINT,
  my_tinyint TINYINT,
)
