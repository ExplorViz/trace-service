package net.explorviz.trace.service.trace;

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


  /**
   * Persist a trace. This may be an update to an already existing trace.
   * @param trace the trace to persist
   */
  void upsert(Trace trace);


}
