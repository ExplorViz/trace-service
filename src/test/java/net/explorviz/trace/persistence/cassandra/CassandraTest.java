package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import java.io.IOException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for all test using an in-memory cassandra database.
 */
public class CassandraTest {

  protected DbHelper db;
  protected CqlSession sess;



  @BeforeAll
  static void beforeAll() throws IOException, InterruptedException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
  }

  @BeforeEach
  void setUpDb() {
    this.sess = EmbeddedCassandraServerHelper.getSession();
    this.db = new DbHelper(this.sess);
    this.db.initialize();
  }

  @AfterEach
  void tearDown() {
    EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra(DbHelper.KEYSPACE_NAME);
  }



}
