
CREATE KEYSPACE IF NOT EXISTS keySpaceTest WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE keySpaceTest;

CREATE TABLE IF NOT EXISTS TRACKER (
  DUMMY_NODE_ID int,
  EVENT_TIMESTAMP timestamp,
  DEVICE_NAME text,
  EVENT_NAME text,
  primary key ((DUMMY_NODE_ID), EVENT_TIMESTAMP, DEVICE_NAME)
);




