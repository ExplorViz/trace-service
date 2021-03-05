package net.explorviz.trace.cassandra;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaTestResource implements QuarkusTestResourceLifecycleManager {

  private static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

  @Override
  public Map<String, String> start() {
    KAFKA.start();
    final Properties config = new Properties();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

    final AdminClient localKafkaAdmin = AdminClient.create(config);

    final int partitions = 1;
    final short replication = 1;
    final NewTopic topicSpans = new NewTopic("explorviz-spans-dynamic", partitions, replication);
    final NewTopic topicTokens = new NewTopic("token-events", partitions, replication);
    final List<NewTopic> topics = Arrays.asList(topicSpans, topicTokens);

    localKafkaAdmin.createTopics(topics);

    return Collections.singletonMap("kafka.bootstrap.servers", KAFKA.getBootstrapServers());
  }

  @Override
  public void stop() {
    KAFKA.stop();
  }
}
