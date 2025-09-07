-- Add migration script here
CREATE TABLE duration_tests(
  my_id UUID PRIMARY KEY,
  my_duration DURATION,
  my_duration_list LIST<DURATION>
)
