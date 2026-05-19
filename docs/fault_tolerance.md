# Kafka Fault Tolerance Test

## Commands

```bash
docker compose up -d
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic sensor-events --partitions 3 --replication-factor 3 || true
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
docker stop kafka1
docker exec kafka2 kafka-topics --bootstrap-server kafka2:29092 --describe --topic sensor-events
docker start kafka1
```

## Before stopping broker

```text
Topic: sensor-events    PartitionCount: 3    ReplicationFactor: 3    Configs: min.insync.replicas=2
Partition: 0    Leader: 3    Replicas: 3,1,2    Isr: 3,1,2
Partition: 1    Leader: 1    Replicas: 1,2,3    Isr: 1,2,3
Partition: 2    Leader: 2    Replicas: 2,3,1    Isr: 2,3,1
```

## After stopping kafka1

```text
Topic: sensor-events    PartitionCount: 3    ReplicationFactor: 3    Configs: min.insync.replicas=2
Partition: 0    Leader: 3    Replicas: 3,1,2    Isr: 3,2
Partition: 1    Leader: 2    Replicas: 1,2,3    Isr: 2,3
Partition: 2    Leader: 2    Replicas: 2,3,1    Isr: 2,3
```

## Interpretation

With RF=3 and min.insync.replicas=2, a single broker failure should not lose data. If the stopped broker was leader for a partition, Kafka elects a new leader from the ISR. The ISR shrinks during failure and expands after the broker rejoins.
