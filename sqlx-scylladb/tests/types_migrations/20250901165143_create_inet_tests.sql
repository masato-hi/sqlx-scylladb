-- Add migration script here
CREATE TABLE inet_tests(
  my_id UUID PRIMARY KEY,
  my_inet INET,
  my_inet_list LIST<INET>,
  my_inet_set SET<INET>
)
