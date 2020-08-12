package net.explorviz.trace.service;

import java.time.Instant;
import java.util.List;
import net.explorviz.avro.Trace;

/**
 * Persistent access of traces.
 */
public interface TraceService {

  /**
   * Find a trace by its trace id.
   * @param landscapeToken the landscape the trace belongs to
   * @param traceId the id of the trace
   * @return the trace with the given id in the given landscape if existing.
   */
  Trace getById(String landscapeToken, String traceId);

  default List<Trace> getFrom(String landscapeToken, long millisFrom) {
    return getBetween(landscapeToken, millisFrom, Instant.now().toEpochMilli());
  }
  List<Trace> getBetween(String landscapeToken, long millisFrom, long millisTo);

}
