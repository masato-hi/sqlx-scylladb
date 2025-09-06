-- Add migration script here
CREATE TABLE timestamp_tests(
  my_id UUID PRIMARY KEY,
  my_timestamp TIMESTAMP,
  my_timestamp_list LIST<TIMESTAMP>,
  my_timestamp_set SET<TIMESTAMP>
)
