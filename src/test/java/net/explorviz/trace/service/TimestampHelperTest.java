package net.explorviz.trace.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import net.explorviz.avro.Timestamp;
import org.junit.jupiter.api.Test;

class TimestampHelperTest {

  @Test
  void order() {
    final Instant now = Instant.now();

    final Timestamp plus5 = TimestampHelper.toTimestamp(now.plus(5, ChronoUnit.SECONDS));
    final Timestamp minus5 = TimestampHelper.toTimestamp(now.minus(5, ChronoUnit.SECONDS));


    // Order
    assertTrue(TimestampHelper.isBefore(minus5, plus5));
    assertTrue(TimestampHelper.isAfter(plus5, minus5));

    // Asymmetric
    assertFalse(TimestampHelper.isBefore(plus5, minus5));
    assertFalse(TimestampHelper.isAfter(minus5, plus5));

    // Not reflexive, i.e., strict order
    assertFalse(TimestampHelper.isAfter(minus5, minus5));
    assertFalse(TimestampHelper.isBefore(minus5, minus5));
  }


  @Test
  void durationMs() {
    final Instant now = Instant.now();

    final long durationMs = 123547;
    final Duration d = Duration.of(durationMs, ChronoUnit.MILLIS);

    final Timestamp start = TimestampHelper.toTimestamp(now);
    final Timestamp end = TimestampHelper.toTimestamp(now.plus(d));

    assertEquals(durationMs, TimestampHelper.durationMs(start, end));
    assertEquals(-durationMs, TimestampHelper.durationMs(end, start));
    assertEquals(0, TimestampHelper.durationMs(start, start));
  }

  @Test
  void conversion() {
    final Instant now = Instant.now();
    final Timestamp ts = TimestampHelper.toTimestamp(now);
    final Instant i = TimestampHelper.toInstant(ts);
    assertEquals(now, i);
  }

}
