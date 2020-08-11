package net.explorviz.persistence.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import java.net.InetSocketAddress;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Producer for preconfigured {@link CqlSession}.
 */
@ApplicationScoped
public class CassandraFactory {

  private final CqlSession session;


  @Inject
  public CassandraFactory(
      @ConfigProperty(name = "explorviz.trace.cassandra.host") final String cassandraHost,
      @ConfigProperty(name = "explorviz.trace.cassandra.port") final int cassandraPort,
      @ConfigProperty(name = "explorviz.trace.cassandra.datacenter") final String datacenter,
      @ConfigProperty(name = "explorviz.trace.cassandra.username") final String username,
      @ConfigProperty(name = "explorviz.trace.cassandra.password") final String password) {
    // TODO read from config values
    final CqlSessionBuilder builder = CqlSession.builder();
    builder.addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort));
    builder.withAuthCredentials(username, password);
    builder.withLocalDatacenter(datacenter);
    this.session = builder.build();
  }

  /**
   * Return a ready for use {@link CqlSession}
   */
  @Produces
  public CqlSession produceCqlSession() {
    return this.session;
  }


}
