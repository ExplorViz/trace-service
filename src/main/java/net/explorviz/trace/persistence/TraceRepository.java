package net.explorviz.trace.persistence;

import java.util.Optional;
import net.explorviz.avro.Trace;

/**
 * Manages (usually persistent) access to a collection of {@link Trace}s.
 */
public interface TraceRepository {

  /**
   * Inserts a new trace.
   *
   * @param trace the trace
   */
  void insert(Trace trace) throws PersistingException;

  /**
   * Updates a trace that already exists in the DB.
   *
   * @param trace the trace to update
   * @throws PersistingException if updating failed
   */
  void update(Trace trace) throws PersistingException;

  /**
   * Finds a trace for a given a landscape token and trace id.
   * @param landscapeToken the landscape token
   * @param traceId the trace id
   * @return an optional containing the trace if existing and is empty otherwise
   */
  Optional<Trace> getTrace(String landscapeToken, String traceId);




}
