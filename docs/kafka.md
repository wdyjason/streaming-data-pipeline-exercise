# Kafka

## Commands

**Bash into kafka container**

```bash
docker-compose exec kafka bash
```

**Kafka CLI**

```bash
kafka-topics --create --topic quickstart-events --bootstrap-server localhost:9092
kafka-topics --describe --topic quickstart-events --bootstrap-server localhost:9092
kafka-topics --list --bootstrap-server localhost:9092

# Without explicit key
kafka-console-producer --topic quickstart-events --broker-list localhost:9092
kafka-console-consumer --topic quickstart-events --from-beginning --bootstrap-server localhost:9092

# With key explicitly set
kafka-console-producer --topic quickstart-events --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=:" --producer-property key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.IntegerSerializer
kafka-console-consumer --topic quickstart-events --from-beginning --bootstrap-server localhost:9092 --property print.key=true
```
