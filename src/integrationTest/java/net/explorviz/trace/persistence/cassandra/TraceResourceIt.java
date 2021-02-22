package net.explorviz.trace.persistence.cassandra;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
// @QuarkusTestResource(CassandraTestResource.class)
// @TestProfile(CassandraTestProfile.class)
public class TraceResourceIt {

  // Tests
  // - insert and retrieve single trace with 5 spans
  // - insert and retrieve 5 traces with 5 spans each
  // - filter by timestamp
  // - get by trace id

  // @Inject
  // private TraceRepository repository;

  @Test
  public void shouldSaveAndRetrieveEntity() {
    // this.repository.insert(TraceHelper.randomTrace(2));
  }

}
