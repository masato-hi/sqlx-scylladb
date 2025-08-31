-- Add migration script here
CREATE TABLE date_tests(
  my_id UUID PRIMARY KEY,
  my_date DATE,
  my_date_list LIST<DATE>,
  my_date_set SET<DATE>
)
