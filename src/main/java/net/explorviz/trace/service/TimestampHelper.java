package net.explorviz.trace.service;

import java.time.Duration;
import java.time.Instant;
import net.explorviz.avro.Timestamp;


/**
 * Helper class for {@link Timestamp}s.
 */
public final class TimestampHelper {

  private TimestampHelper() { /* Nothing to do */}

  /**
   * Checks if the first timestamp is before than the second or the same.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first <= second
   */
  public static boolean isBeforeOrEqual(final Timestamp one, final Timestamp two) {
    return isBefore(one, two) || isEqual(one, two);
  }

  /**
   * Checks if the first timestamp is before than the second.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first < second
   */
  public static boolean isBefore(final Timestamp one, final Timestamp two) {
    final Instant f = toInstant(one);
    final Instant s = toInstant(two);
    return f.isBefore(s);
  }

  /**
   * Checks if the first timestamp is after than the second.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first > second
   */
  public static boolean isAfter(final Timestamp one, final Timestamp two) {
    final Instant f = toInstant(one);
    final Instant s = toInstant(two);
    return f.isAfter(s);
  }

  /**
   * Checks if the first timestamp is after than the second or the same.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first >= second
   */
  public static boolean isAfterOrEqual(final Timestamp one, final Timestamp two) {
    return isAfter(one, two) || isEqual(one, two);
  }

  public static boolean isEqual(final Timestamp one, final Timestamp two) {
    final Instant f = toInstant(one);
    final Instant s = toInstant(two);
    return f.equals(s);
  }

  /**
   * Calculates the duration between two timestamps in milliseconds.
   *
   * @param t1 the first timestamp
   * @param t2 the second timestamp
   * @return the duration in milliseconds
   */
  public static long durationMs(final Timestamp t1, final Timestamp t2) {
    final Instant s = toInstant(t1);
    final Instant e = toInstant(t2);
    return Duration.between(s, e).toMillis();
  }

  /**
   * Converts a timestamp to an instant. Converse of {@link #toTimestamp(Instant)}.
   *
   * @param t the timestamp
   * @return an instant representing the exact same time as the timestamp
   */
  public static Instant toInstant(final Timestamp t) {
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanoAdjust());
  }

  /**
   * Converts an instant to a timestamp. Converse of {@link #toInstant(Timestamp)}.
   *
   * @param i the instant
   * @return a timestamp representing the exact same time as the instant
   */
  public static Timestamp toTimestamp(final Instant i) {
    return new Timestamp(i.getEpochSecond(), i.getNano());
  }

}
