package net.explorviz.trace.kafka;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Configuration options for Kafka.
 */
@ApplicationScoped
@SuppressWarnings("PMD.DefaultPackage")
public class KafkaConfig {

  private static final Class<SpanTimestampKafkaExtractor> TIMESTAMP_EXTRACTOR =
      SpanTimestampKafkaExtractor.class;

  // CHECKSTYLE:OFF

  @ConfigProperty(name = "quarkus.kafka-streams.application-id")
  /* default */ String applicationId;

  @ConfigProperty(name = "quarkus.kafka-streams.bootstrap-servers")
  /* default */ String bootstrapServers;

  @ConfigProperty(name = "explorviz.kafka-streams.topics.in")
  /* default */ String inTopic;

  @ConfigProperty(name = "explorviz.schema-registry.url")
  /* default */ String schemaRegistryUrl;

  @ConfigProperty(name = "explorviz.commit-interval-ms")
  /* default */ int commitIntervalMs;

  @ConfigProperty(name = "explorviz.kafka-streams.max-record-size")
  /* default */ int maxRecordSize;

  // CHECKSTYLE:ON

  public int getCommitIntervalMs() {
    return this.commitIntervalMs;
  }

  public Class<SpanTimestampKafkaExtractor> getTimestampExtractor() {
    return KafkaConfig.TIMESTAMP_EXTRACTOR;
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
