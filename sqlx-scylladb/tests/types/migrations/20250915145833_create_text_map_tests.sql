-- Add migration script here
CREATE TABLE text_map_tests(
  my_id UUID PRIMARY KEY,
  my_ascii_ascii MAP<ASCII, ASCII>,
  my_text_text MAP<TEXT, TEXT>,
  my_text_boolean MAP<TEXT, BOOLEAN>,
  my_text_tinyint MAP<TEXT, TINYINT>,
  my_text_smallint MAP<TEXT, SMALLINT>,
  my_text_int MAP<TEXT, INT>,
  my_text_bigint MAP<TEXT, BIGINT>,
  my_text_float MAP<TEXT, FLOAT>,
  my_text_double MAP<TEXT, DOUBLE>,
  my_text_uuid MAP<TEXT, UUID>,
  my_text_inet MAP<TEXT, INET>,
)
