package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;
import net.explorviz.trace.persistence.PersistingException;
import net.explorviz.trace.persistence.SpanRepository;

@ApplicationScoped
public class NoopRepo implements SpanRepository {
  @Override
  public void insert(final SpanDynamic span) throws PersistingException {

  }

  @Override
  public void saveTrace(final Trace trace) throws PersistingException {

  }

  @Override
  public CompletionStage<AsyncResultSet> saveTraceAsync(final Trace trace)
      throws PersistingException {
    return null;
  }

  @Override
  public Optional<Collection<SpanDynamic>> getSpans(final String landscapeToken,
                                                    final String traceId) {
    return Optional.empty();
  }

  @Override
  public List<Set<SpanDynamic>> getAllInRange(final String landscapeToken, final Instant from,
                                              final Instant to) {
    return null;
  }

  @Override
  public void deleteAll(final String landscapeToken) {

  }
}
