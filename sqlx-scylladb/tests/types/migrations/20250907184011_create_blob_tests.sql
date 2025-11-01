-- Add migration script here
CREATE TABLE blob_tests(
  my_id UUID PRIMARY KEY,
  my_blob BLOB,
  my_blob_list LIST<BLOB>,
  my_blob_set SET<BLOB>
)
