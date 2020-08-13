package net.explorviz.trace.service;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import jnr.ffi.annotations.In;
import net.explorviz.avro.Timestamp;
import org.apache.groovy.json.internal.Chr;
import org.junit.jupiter.api.Test;

class TimestampHelperTest {

  @Test
  void order() {
    Instant now = Instant.now();

    Timestamp plus5 = TimestampHelper.toTimestamp(now.plus(5, ChronoUnit.SECONDS));
    Timestamp minus5 = TimestampHelper.toTimestamp(now.minus(5, ChronoUnit.SECONDS));


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
    Instant now = Instant.now();

    long durationMs = 123547;
    Duration d = Duration.of(durationMs, ChronoUnit.MILLIS);

    Timestamp start = TimestampHelper.toTimestamp(now);
    Timestamp end = TimestampHelper.toTimestamp(now.plus(d));

    assertEquals(durationMs, TimestampHelper.durationMs(start, end));
    assertEquals(-durationMs, TimestampHelper.durationMs(end, start));
    assertEquals(0, TimestampHelper.durationMs(start, start));
  }

  @Test
  void conversion() {
    Instant now = Instant.now();
    Timestamp ts = TimestampHelper.toTimestamp(now);
    Instant i = TimestampHelper.toInstant(ts);
    assertEquals(now, i);
  }

}
