package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.quarkus.test.CassandraTestResource;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.wait.CassandraQueryWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class CassandraCustomTestResource extends CassandraTestResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTestResource.class);

  private static CassandraContainer<?> cassandraContainer;

  @Override
  public Map<String, String> start() {
    cassandraContainer =
        new CassandraContainer<>(DockerImageName.parse("cassandra:3.11.10"));
    // set init script only if it's provided by the caller
    final URL resource =
        Thread.currentThread().getContextClassLoader().getResource("init_script.cql");
    if (resource != null) {
      cassandraContainer.withInitScript("init_script.cql");
    }
    cassandraContainer.setWaitStrategy(new CassandraQueryWaitStrategy());
    cassandraContainer.start();
    final String exposedPort =
        String.valueOf(cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT));
    String exposedHost = cassandraContainer.getContainerIpAddress();
    if (exposedHost.equals("localhost")) {
      exposedHost = "127.0.0.1";
    }
    LOGGER.info(
        "Started {} on {}:{}", cassandraContainer.getDockerImageName(), exposedHost, exposedPort);
    final HashMap<String, String> result = new HashMap<>();
    result.put("quarkus.cassandra.docker_host", exposedHost);
    result.put("quarkus.cassandra.docker_port", exposedPort);
    return result;
  }

  @Override
  public void stop() {
    if (cassandraContainer != null && cassandraContainer.isRunning()) {
      cassandraContainer.stop();
    }
  }

}
