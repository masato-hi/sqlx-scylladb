-- Add up migration script here
CREATE TABLE test_counter(
  my_test_id BIGINT PRIMARY KEY,
  my_counter COUNTER,
);
