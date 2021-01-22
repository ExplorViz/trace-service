package net.explorviz.trace.kafka;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Configuration options for Kafka.
 */
@ApplicationScoped
// https://quarkus.io/guides/config#using-configproperties
public class KafkaConfig {

  private static final Class<SpanTimestampKafkaExtractor> TIMESTAMP_EXTRACTOR =
      SpanTimestampKafkaExtractor.class;

  @ConfigProperty(name = "quarkus.kafka-streams.application-id")
  String applicationId; // NOCS


  @ConfigProperty(name = "quarkus.kafka-streams.bootstrap-servers")
  String bootstrapServers; // NOCS

  @ConfigProperty(name = "explorviz.kafka-streams.topics.in")
  String inTopic; // NOCS

  @ConfigProperty(name = "explorviz.kafka-streams.topics.out")
  String outTopic; // NOCS

  @ConfigProperty(name = "explorviz.schema-registry.url")
  String schemaRegistryUrl; // NOCS

  @ConfigProperty(name = "explorviz.commit-interval-ms")
  int commitIntervalMs; // NOCS

  public int getCommitIntervalMs() {
    return this.commitIntervalMs;
  }

  public Class<SpanTimestampKafkaExtractor> getTimestampExtractor() {
    return this.TIMESTAMP_EXTRACTOR;
  }

  public String getApplicationId() {
    return this.applicationId;
  }

  public String getBootstrapServers() {
    return this.bootstrapServers;
  }

  public String getInTopic() {
    return this.inTopic;
  }

  public String getSchemaRegistryUrl() {
    return this.schemaRegistryUrl;
  }

}
