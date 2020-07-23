# ksql-udaf-delta
ksqlDB UDAF that calculates the delta between two consecutive records for a key. 

### Example
create tables
```
CREATE STREAM TEST_DATA_INTEGER (account string, security string, amount int)
    WITH (kafka_topic='test-data-integer', value_format='json', partitions=1);

CREATE TABLE TEST_DATA_INTEGER_DELTA as SELECT account, security, DELTA(amount) as delta FROM
    TEST_DATA_INTEGER GROUP BY account, security;
```
insert some data
```
INSERT INTO TEST_DATA_INTEGER (account, security, amount) VALUES ('AAA', 'xyz', 5);
INSERT INTO TEST_DATA_INTEGER (account, security, amount) VALUES ('AAA', 'xyz', 10);
INSERT INTO TEST_DATA_INTEGER (account, security, amount) VALUES ('AAA', 'xyz', 55);
INSERT INTO TEST_DATA_INTEGER (account, security, amount) VALUES ('AAA', 'xyz', 35);
```

results:
```
 ksql> select * from TEST_DATA_INTEGER_DELTA emit changes;

+-------------------------------+-------------------------------+-------------------------------+-------------------------------+-------------------------------+
|ROWTIME                        |ROWKEY                         |ACCOUNT                        |SECURITY                       |DELTA                          |
+-------------------------------+-------------------------------+-------------------------------+-------------------------------+-------------------------------+
|1595519341324                  |AAA|+|xyz                      |AAA                            |xyz                            |5                              |
|1595519341489                  |AAA|+|xyz                      |AAA                            |xyz                            |5                              |
|1595519341661                  |AAA|+|xyz                      |AAA                            |xyz                            |45                             |
|1595519341775                  |AAA|+|xyz                      |AAA                            |xyz                            |-20                            |
```
