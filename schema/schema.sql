CREATE TABLE "items" (
  id   uuid,
  name text,
  PRIMARY KEY(id),
);

CREATE TABLE "item_descriptions" (
  id      uuid,
  item_id uuid,
  name text,

  CONSTRAINT fk_item
      FOREIGN KEY(item_id)
      REFERENCES items(id)
);
