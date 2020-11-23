package net.explorviz.trace.service;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import net.explorviz.avro.Trace;

/**
 * Access traces.
 */
public interface TraceService {

  /**
   * Find a trace by its trace id.
   *
   * @param landscapeToken the landscape the trace belongs to
   * @param traceId        the id of the trace
   * @return the trace with the given id in the given landscape if existing.
   */
  Optional<Trace> getById(String landscapeToken, String traceId);

  /**
   * Get all traces of a specific landscape that occurred after a given point in time.
   *
   * @param landscapeToken the token of the landscape
   * @param from           the timestamp from which on to retrieve traces (inclusive)
   * @return all traces occurred after the given timestamp
   */
  default Collection<Trace> getFrom(String landscapeToken, Instant from) {
    return getBetween(landscapeToken, from, Instant.now());
  }

  /**
   * Get all traces of a landscape that occurred between two timestamp.
   *
   * @param landscapeToken the token of the landscape
   * @param millisFrom     the lower bound timestamp (inclusive)
   * @param millisTo       the upper bound timestamp (inclusive)
   * @return all traces occurred within the given timestamps
   */
  Collection<Trace> getBetween(String landscapeToken, Instant millisFrom, Instant millisTo);


  /**
   * Deletes all traces associated to a landscape token.
   *
   * @param landscapeToken the token to delete all traces for
   */
  void deleteAll(String landscapeToken);

}
