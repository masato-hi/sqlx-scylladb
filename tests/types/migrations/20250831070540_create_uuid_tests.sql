-- Add migration script here
CREATE TABLE uuid_tests(
  my_id UUID PRIMARY KEY,
  my_uuid UUID,
  my_uuid_list LIST<UUID>,
  my_uuid_set SET<UUID>
)
