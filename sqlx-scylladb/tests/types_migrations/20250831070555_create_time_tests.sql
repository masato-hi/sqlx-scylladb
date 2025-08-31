-- Add migration script here
CREATE TABLE time_tests(
  my_id UUID PRIMARY KEY,
  my_time TIME,
  my_time_list LIST<TIME>,
  my_time_set SET<TIME>
)
