-- Add migration script here
CREATE TABLE text_tests(
  my_id UUID PRIMARY KEY,
  my_text TEXT,
  my_text_list LIST<TEXT>,
  my_text_set SET<TEXT>
)
