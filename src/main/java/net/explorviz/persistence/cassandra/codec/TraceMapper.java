package net.explorviz.persistence.cassandra.codec;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.Map;
import net.explorviz.avro.Trace;

public class TraceMapper implements ValueMapper<Trace> {

  @Override
  public Map<String, Term> toMap(final Trace item) {
    return null;
  }

  @Override
  public Trace fromRow(final Row row) {
    return null;
  }
}
