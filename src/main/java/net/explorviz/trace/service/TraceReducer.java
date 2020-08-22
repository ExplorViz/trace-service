package net.explorviz.trace.service;

import java.util.ArrayList;
import java.util.List;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;

/**
 * Reduces the amount of spans in a trace. This is necessary to reduce data volume.
 */
public class TraceReducer {


  public Trace reduce(Trace original) {
    return reduceLoops(reduceRecursion(original));
  }

  private Trace reduceLoops(Trace original) {
    List<SpanDynamic> spans = original.getSpanList();
    List<SpanDynamic> reducedSpans = new ArrayList<>(spans.size());
    for (int i = 0; i < spans.size(); i++) {
      SpanDynamic current = spans.get(i);
      // Reduce subsequent similar spans to one
      int j = 1;
      boolean reduction = true; // Sentinel
      while (j < spans.size() - i && reduction) {
        SpanDynamic next = spans.get(i + j);
        if (isLoop(current, next) ) {
          j++;
          current.setEndTime(next.getEndTime());
        } else {
          reduction = false;
        }
      }
      // Reduce j-1 spans to a single span
      reducedSpans.add(current);
      // Skip the next j-1 spans a they are already reduced
      i = i + (j - 1);

    }

    return Trace.newBuilder(original).setSpanList(reducedSpans).build();
  }

  private Trace reduceRecursion(Trace original) {
    List<SpanDynamic> spans = original.getSpanList();
    List<SpanDynamic> reducedSpans = new ArrayList<>(spans.size());
    for (int i = 0; i < spans.size(); i++) {
      SpanDynamic current = spans.get(i);
      SpanDynamic temp = current;
      // Reduce subsequent recursion
      int j = 1;
      boolean canReduce = true; // Sentinel
      while (j < spans.size() - i && canReduce) {
        SpanDynamic next = spans.get(i + j);
        if (isRecursion(temp, next) ) {
          j++;
          temp = next; // Check if recursion proceeds
          current.setEndTime(next.getEndTime());
        } else {
          canReduce = false;
        }
      }
      // Reduce j-1 spans to a single span
      reducedSpans.add(current);
      // Skip the next j-1 spans a they are already reduced
      i = i + (j - 1);

    }

    return Trace.newBuilder(original).setSpanList(reducedSpans).build();
  }


  /**
   * Checks is two spans are part of a loop. Spans are looped if
   * (1) they have refer to the same method called and
   * (2) their caller is the same.
   *
   * @param s1 the first span
   * @param s2 the second span
   * @return {@code true} iff s1 and s2 are similar by above definition
   */
  private boolean isLoop(SpanDynamic s1, SpanDynamic s2) {
    return s1.getHashCode().equals(s2.getHashCode())
        && s1.getParentSpanId().equals(s2.getParentSpanId());
  }

  /**
   * Checks if two spans are part of a (direct) recursion. Spans are part of a direct recursion if
   * (1) they refer to the same method
   * (2) the parent of the first span is the second span or vice versa
   *
   * @param s1 the first span
   * @param s2 the second span
   * @return {@code true} iff s1 and s2 are part of a direct recursion
   */
  private boolean isRecursion(SpanDynamic s1, SpanDynamic s2) {
    return s1.getHashCode().equals(s2.getHashCode())
        && (s1.getParentSpanId().equals(s2.getSpanId())
        || s2.getParentSpanId().equals(s1.getSpanId()));
  }


}
