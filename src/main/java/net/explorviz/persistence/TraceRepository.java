package net.explorviz.persistence;

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





}
