-- Add migration script here
CREATE TABLE IF NOT EXISTS users(
  id BIGINT PRIMARY KEY,
  name TEXT,
)
