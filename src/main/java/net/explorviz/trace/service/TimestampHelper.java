package net.explorviz.trace.service;

import java.time.Duration;
import java.time.Instant;
import net.explorviz.avro.Timestamp;


/**
 * Helper class for {@link Timestamp}s.
 */
public class TimestampHelper {

  private TimestampHelper() {
  }

  /**
   * Checks if the first timestamp is before than the second or the same.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first <= second
   */
  public static boolean isBeforeOrEqual(Timestamp one, Timestamp two) {
    return isBefore(one, two) || isEqual(one, two);
  }

  /**
   * Checks if the first timestamp is before than the second.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first < second
   */
  public static boolean isBefore(Timestamp one, Timestamp two) {
    Instant f = toInstant(one);
    Instant s = toInstant(two);
    return f.isBefore(s);
  }

  /**
   * Checks if the first timestamp is after than the second.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first > second
   */
  public static boolean isAfter(Timestamp one, Timestamp two) {
    Instant f = toInstant(one);
    Instant s = toInstant(two);
    return f.isAfter(s);
  }

  /**
   * Checks if the first timestamp is after than the second or the same.
   *
   * @param one the first timestamp
   * @param two the second timestamp
   * @return true iff first >= second
   */
  public static boolean isAfterOrEqual(Timestamp one, Timestamp two) {
    return isAfter(one, two) || isEqual(one, two);
  }

  public static boolean isEqual(Timestamp one, Timestamp two) {
    Instant f = toInstant(one);
    Instant s = toInstant(two);
    return f.equals(s);
  }

  /**
   * Calculates the duration between two timestamps in milliseconds.
   *
   * @param t1 the first timestamp
   * @param t2 the second timestamp
   * @return the duration in milliseconds
   */
  public static long durationMs(Timestamp t1, Timestamp t2) {
    Instant s = toInstant(t1);
    Instant e = toInstant(t2);
    return Duration.between(s, e).toMillis();
  }

  /**
   * Converts a timestamp to an instant. Converse of {@link #toTimestamp(Instant)}.
   *
   * @param t the timestamp
   * @return an instant representing the exact same time as the timestamp
   */
  public static Instant toInstant(Timestamp t) {
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanoAdjust());
  }

  /**
   * Converts an instant to a timestamp. Converse of {@link #toInstant(Timestamp)}.
   *
   * @param i the instant
   * @return a timestamp representing the exact same time as the instant
   */
  public static Timestamp toTimestamp(Instant i) {
    return new Timestamp(i.getEpochSecond(), i.getNano());
  }

}
