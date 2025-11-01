-- Add migration script here
CREATE TABLE IF NOT EXISTS example_any(
  id BIGINT PRIMARY KEY,
  any_map MAP<BIGINT, TEXT>,
)
