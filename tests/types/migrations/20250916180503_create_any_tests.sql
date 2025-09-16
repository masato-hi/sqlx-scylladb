-- Add migration script here
CREATE TABLE any_tests(
  my_id UUID PRIMARY KEY,
  my_any MAP<UUID, TIMESTAMP>
)
