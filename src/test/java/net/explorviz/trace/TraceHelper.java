package net.explorviz.trace;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import net.explorviz.trace.service.TimestampHelper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

/**
 * Utility class containing helper methods to create traces and spans.
 */
public final class TraceHelper {


  /**
   * Shortcut for {@link #randomSpan(String, String)} with random trace id and landscape token.
   */
  public static SpanDynamic randomSpan() {
    String randomTraceId = RandomStringUtils.random(6, true, true);
    String landscapeToken = RandomStringUtils.random(32, true, true);
    return randomSpan(randomTraceId, landscapeToken);
  }

  /**
   * Create a random span with given trace id and landscape token such that:
   * <ul>
   *   <li>Timestamps are random points in time in the year of 2020 (to avoid overflows)</li>
   *   <li>Parent span IDs are completely random Ids</li>
   *   <li>Hash codes are not calculated but random strings</li>
   * </ul>
   *
   * @param traceId the trace id to use
   * @param token   the token to use
   * @return a randomly generated span
   */
  public static SpanDynamic randomSpan(String traceId, String token) {
    long maxSeconds = 1609459200;
    long minSeconds = 1577836800;
    int maxNanos = Instant.MAX.getNano();

    return SpanDynamic.newBuilder()
        .setLandscapeToken(token)
        .setStartTime(new Timestamp(RandomUtils.nextLong(minSeconds, maxSeconds),
            RandomUtils.nextInt(0, maxNanos)))
        .setEndTime(new Timestamp(RandomUtils.nextLong(minSeconds, maxSeconds),
            RandomUtils.nextInt(0, maxNanos)))
        .setTraceId(traceId)
        .setParentSpanId(RandomStringUtils.random(8, true, true))
        .setSpanId(RandomStringUtils.random(8, true, true))
        .setHashCode(RandomStringUtils.random(256, true, true))
        .build();
  }

  /**
   * Creates a random trace such that:
   * <ul>
   *   <li>The start- and end-time correspond to the span lis</li>
   *   <li>All spans have the same traceId</li>
   *   <li>The trace and all spans have the same landscape token</li>
   * </ul>
   * The included spans themselves are not valid, in particular
   * <ul>
   *   <li>Parent span IDs are completely random do not match spans in the same trace</li>
   *   <li>The hash codes are truly random</li>
   *   <li>Start- and End-Times are random points anywhere in the year 2020</li>
   * </ul>
   *
   * @param spanAmount the amount of spans to include into the trace, must be at least 1
   * @return a trace with randomly filled values
   */
  public static Trace randomTrace(int spanAmount) {

    String traceId = RandomStringUtils.random(6, true, true);
    String landscapeToke = RandomStringUtils.random(32, true, true);

    Timestamp start = null;
    Timestamp end = null;
    List<SpanDynamic> spans = new ArrayList<>();
    for (int i = 0; i < spanAmount; i++) {
      SpanDynamic s = randomSpan(traceId, landscapeToke);
      if (start == null || TimestampHelper.isBefore(s.getStartTime(), start)) {
        start = s.getStartTime();
      }
      if (end == null || TimestampHelper.isAfter(s.getEndTime(), end)) {
        end = s.getEndTime();
      }
      spans.add(s);
    }

    return Trace.newBuilder()
        .setLandscapeToken(landscapeToke)
        .setTraceId(traceId)
        .setStartTime(start)
        .setEndTime(end)
        .setDuration(TimestampHelper.durationMs(start, end))
        .setSpanList(spans)
        .setTraceCount(1)
        .setOverallRequestCount(1)
        .build();
  }


}
