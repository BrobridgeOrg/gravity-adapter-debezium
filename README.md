# gravity-adapter-debezium

Gravity adapter for debezium CDC framework.

## Troubleshooting

### Snapshot doesn't work

Debezium has implementation of initial load which is called snapshot, but somehow snapshot doesn't work when adapter starts. The root cause of this problem is the topic already exists or offsets of consumer group is not zero. That means you probably start adapter in the past.

The solution is to delete existing consumer group and topic. In order to fix this problem, you can using kafka command in the following:

```shell
# Delete specific topic for datasource table in PostgreSQL
kafka-topics --delete --bootstrap-server localhost:29092 --topic postgres_0.public.datasource

# Delete consumer group for gravity adapter
kafka-consumer-groups --bootstrap-server localhost:29092 --delete --group gravity_adapter_debezium-mylaptop-debezium
```


