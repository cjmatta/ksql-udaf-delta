CREATE STREAM TEST_DATA_INTEGER (account string, security string, amount int)
    WITH (kafka_topic='test-data-integer', value_format='json', partitions=1);

INSERT INTO TEST_DATA_INTEGER (account, security, amount) VALUES ('AAA', 'xyz', 5);
INSERT INTO TEST_DATA_INTEGER (account, security, amount) VALUES ('AAA', 'xyz', 10);
INSERT INTO TEST_DATA_INTEGER (account, security, amount) VALUES ('AAA', 'xyz', 55);
INSERT INTO TEST_DATA_INTEGER (account, security, amount) VALUES ('AAA', 'xyz', 35);

CREATE TABLE TEST_DATA_INTEGER_DELTA as SELECT account, security, DELTA(amount) as delta FROM
    TEST_DATA_INTEGER GROUP BY account, security;


CREATE STREAM TEST_DATA_LONG (account string, security string, amount bigint)
    WITH (kafka_topic='test-data-long', value_format='json', partitions=1);

INSERT INTO TEST_DATA_LONG (account, security, amount) VALUES ('AAA', 'xyz', 5);
INSERT INTO TEST_DATA_LONG (account, security, amount) VALUES ('AAA', 'xyz', 10);
INSERT INTO TEST_DATA_LONG (account, security, amount) VALUES ('AAA', 'xyz', 55);
INSERT INTO TEST_DATA_LONG (account, security, amount) VALUES ('AAA', 'xyz', 35);

CREATE TABLE TEST_DATA_LONG_DELTA as SELECT account, security, DELTA(amount) as delta FROM
    TEST_DATA_LONG GROUP BY account, security;



