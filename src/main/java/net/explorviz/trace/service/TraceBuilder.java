package net.explorviz.trace.service;

import java.util.Collection;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;


/**
 * Builds a {@link Trace} out of a collection of {@link SpanDynamic}s.
 */
public interface TraceBuilder {

  /**
   * Builds the {@link Trace} that groups the the collection of {@link SpanDynamic}s.
   *
   * @param spans the
   * @return the trace corresponding to the spans
   */
  Trace build(Collection<SpanDynamic> spans);

}
