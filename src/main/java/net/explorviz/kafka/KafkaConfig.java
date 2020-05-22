package net.explorviz.kafka;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
// https://quarkus.io/guides/config#using-configproperties
public class KafkaConfig {

  private final Class<EVSpanTimestampKafkaExtractor> TIMESTAMP_EXTRACTOR =
      EVSpanTimestampKafkaExtractor.class;

  @ConfigProperty(name = "quarkus.kafka-streams.application-id")
  String applicationId;

  @ConfigProperty(name = "quarkus.kafka-streams.bootstrap-servers")
  String bootstrapServers;

  @ConfigProperty(name = "quarkus.kafka-streams.topics")
  String inTopic;

  @ConfigProperty(name = "explorviz.kafka-streams.topics.out")
  String outTopic;

  @ConfigProperty(name = "explorviz.schema-registry.url")
  String schemaRegistryUrl;
  
  @ConfigProperty(name = "explorviz.commit-interval-ms")
  int commitIntervalMs;

  public int getCommitIntervalMs() {
    return commitIntervalMs;
  }

  public void setCommitIntervalMs(int commitIntervalMs) {
    this.commitIntervalMs = commitIntervalMs;
  }

  public Class<EVSpanTimestampKafkaExtractor> getTimestampExtractor() {
    return TIMESTAMP_EXTRACTOR;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public String getInTopic() {
    return inTopic;
  }

  public String getOutTopic() {
    return outTopic;
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

}
