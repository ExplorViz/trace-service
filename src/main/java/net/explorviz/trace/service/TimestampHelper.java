package net.explorviz.trace.service;

import java.time.Duration;
import java.time.Instant;
import net.explorviz.avro.Timestamp;


/**
 * Helper class for {@link Timestamp}s.
 */
public class TimestampHelper {

  private TimestampHelper(){}

  /**
   * Checks if the first timestamp is before than the second.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first < second
   */
  public static boolean isBefore(Timestamp one, Timestamp two) {
    Instant f = Instant.ofEpochSecond(one.getSeconds(), one.getNanoAdjust());
    Instant s = Instant.ofEpochSecond(two.getSeconds(), two.getNanoAdjust());
    return f.isBefore(s);
  }

  /**
   * Checks if the first timestamp is after than the second.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first < second
   */
  public static boolean isAfter(Timestamp one, Timestamp two) {
    Instant f = Instant.ofEpochSecond(one.getSeconds(), one.getNanoAdjust());
    Instant s = Instant.ofEpochSecond(two.getSeconds(), two.getNanoAdjust());
    return f.isAfter(s);
  }

  /**
   * Calculates the duration between two timestamps in milliseconds
   * @param t1 the first timestamp
   * @param t2 the second timestamp
   * @return the duration in milliseconds
   */
  public static long durationMs(Timestamp t1, Timestamp t2) {
    Instant s = Instant.ofEpochSecond(t1.getSeconds(), t1.getNanoAdjust());
    Instant e = Instant.ofEpochSecond(t2.getSeconds(), t2.getNanoAdjust());
    return Duration.between(s, e).toMillis();
  }
}
