package net.explorviz.trace.persistence.cassandra;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Collections;
import java.util.Map;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaTestResource implements QuarkusTestResourceLifecycleManager {

  private static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));


  @Override
  public Map<String, String> start() {
    KAFKA.start();
    // System.out.println("ALEX " + KAFKA.getBootstrapServers());
    return Collections.singletonMap("kafka.bootstrap.servers", KAFKA.getBootstrapServers());
  }

  @Override
  public void stop() {
    KAFKA.stop();
  }
}
