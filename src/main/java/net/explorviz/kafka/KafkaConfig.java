package net.explorviz.kafka;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
// https://quarkus.io/guides/config#using-configproperties
public class KafkaConfig {

  public static final String TRACES_STORE = "traces";

  private final Class<EVSpanTimestampKafkaExtractor> TIMESTAMP_EXTRACTOR =
      EVSpanTimestampKafkaExtractor.class;

  @ConfigProperty(name = "quarkus.kafka-streams.application-id")
  String applicationId;

  @ConfigProperty(name = "quarkus.kafka-streams.bootstrap-servers")
  String bootstrapServers;

  @ConfigProperty(name = "explorviz.kafka-streams.topics.in")
  String inTopic;

  @ConfigProperty(name = "explorviz.kafka-streams.topics.out")
  String outTopic;

  @ConfigProperty(name = "explorviz.schema-registry.url")
  String schemaRegistryUrl;

  @ConfigProperty(name = "explorviz.commit-interval-ms")
  int commitIntervalMs;

  public int getCommitIntervalMs() {
    return this.commitIntervalMs;
  }

  public void setCommitIntervalMs(final int commitIntervalMs) {
    this.commitIntervalMs = commitIntervalMs;
  }

  public Class<EVSpanTimestampKafkaExtractor> getTimestampExtractor() {
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

  public String getOutTopic() {
    return this.outTopic;
  }

  public String getSchemaRegistryUrl() {
    return this.schemaRegistryUrl;
  }

}
