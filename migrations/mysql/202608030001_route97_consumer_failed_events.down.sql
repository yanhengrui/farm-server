-- Route 9.7 rollback: remove the durable Kafka consumer failure parking table.
DROP TABLE IF EXISTS consumer_failed_events;
