package net.explorviz.trace.persistence;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;

/**
 * Manages (usually persistent) access to a collection of {@link Trace}s.
 */
public interface SpanRepository {

  /**
   * Inserts a new span.
   *
   * @param span the span to save
   */
  void insert(SpanDynamic span) throws PersistingException;

  void saveTrace(Trace trace) throws PersistingException;

  /**
   * Finds a trace for a given a landscape token and trace id.
   * @param landscapeToken the landscape token
   * @param traceId the trace id
   * @return an optional containing the trace if existing and is empty otherwise
   */
  Optional<Collection<SpanDynamic>> getSpans(String landscapeToken, String traceId);


  /**
   * Returns all traces for a specific landscape between a given range
   * @param landscapeToken the landscape token
   * @param from the (inclusive) time of the earliest trace
   * @param to the (inclusive) time of the latest trace
   * @return list of sets of spans
   */
  List<Set<SpanDynamic>> getAllInRange(String landscapeToken, Instant from, Instant to);

  /**
   * Delete all traces for a given landscape token
   * @param landscapeToken the landscape token
   */
  void deleteAll(String landscapeToken);

}
