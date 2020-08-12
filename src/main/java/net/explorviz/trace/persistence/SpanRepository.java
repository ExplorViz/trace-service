package net.explorviz.trace.persistence;

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


  /**
   * Finds a trace for a given a landscape token and trace id.
   * @param landscapeToken the landscape token
   * @param traceId the trace id
   * @return an optional containing the trace if existing and is empty otherwise
   */
  Optional<Set<SpanDynamic>> getSpans(String landscapeToken, String traceId);




}
