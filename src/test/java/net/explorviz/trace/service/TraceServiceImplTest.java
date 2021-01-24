package net.explorviz.trace.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import net.explorviz.trace.TraceHelper;
import net.explorviz.trace.persistence.SpanRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

class TraceServiceImplTest {


  private TraceServiceImpl service;
  private TraceBuilder builder;
  private SpanRepository repo;

  @BeforeEach
  void setUp() {
    this.repo = Mockito.mock(SpanRepository.class);
    this.builder = new TraceBuilderImpl();
    this.service = new TraceServiceImpl(this.repo, this.builder);
  }

  @Test
  void getById() {
    final int spanAmount = 10;
    final Trace testTrace = TraceHelper.randomTrace(spanAmount);
    Mockito.when(this.repo.getSpans(testTrace.getLandscapeToken(), testTrace.getTraceId()))
        .thenReturn(Optional.of(testTrace.getSpanList()));

    final Trace got =
        this.service.getById(testTrace.getLandscapeToken(), testTrace.getTraceId()).orElseThrow();

    assertEquals(testTrace, got);
  }

  @Test
  void getUnknownId() {
    final int spanAmount = 10;
    final Trace testTrace = TraceHelper.randomTrace(spanAmount);
    Mockito.when(this.repo.getSpans(testTrace.getLandscapeToken(), testTrace.getTraceId()))
        .thenReturn(Optional.of(testTrace.getSpanList()));
    Mockito.when(this.repo.getSpans(Matchers.anyString(), Matchers.anyString()))
        .thenReturn(Optional.empty());

    final Optional<Trace> gotUnknownToken =
        this.service.getById("sometoken", testTrace.getTraceId());
    final Optional<Trace> gotUnknownTraceId =
        this.service.getById(testTrace.getLandscapeToken(), "someid");

    assertTrue(gotUnknownToken.isEmpty());
    assertTrue(gotUnknownTraceId.isEmpty());
  }

  @Test
  void getBetween() {
    final int spanAmount = 10;
    // Create 4 traces of the same token in the temporal order
    // t4, t1, t2, t3
    final String token = "token";
    final Instant now = Instant.now();
    final Trace t1 = TraceHelper.randomTrace(spanAmount);
    t1.setLandscapeToken(token);
    t1.setStartTime(TimestampHelper.toTimestamp(now.minus(10, ChronoUnit.SECONDS)));
    final Trace t2 = TraceHelper.randomTrace(spanAmount);
    t2.setLandscapeToken(token);
    t2.setStartTime(TimestampHelper.toTimestamp(now.minus(5, ChronoUnit.SECONDS)));
    final Trace t3 = TraceHelper.randomTrace(spanAmount);
    t3.setLandscapeToken(token);
    t3.setStartTime(TimestampHelper.toTimestamp(now));
    final Trace t4 = TraceHelper.randomTrace(spanAmount);
    t4.setLandscapeToken(token);
    t4.setStartTime(TimestampHelper.toTimestamp(now.minus(20, ChronoUnit.SECONDS)));

    final Set<Trace> traces = new HashSet<>(Arrays.asList(t1, t2, t3, t4));
    // Add random traces
    for (int i = 0; i < 0; i++) {
      traces.add(TraceHelper.randomTrace(spanAmount));
    }

    Mockito.when(this.repo.getAllInRange(Matchers.anyString(), Matchers.any(), Matchers.any()))
        .thenAnswer(i -> {
          final String tok = i.getArgumentAt(0, String.class);
          final Timestamp from = TimestampHelper.toTimestamp(i.getArgumentAt(1, Instant.class));
          final Timestamp to = TimestampHelper.toTimestamp(i.getArgumentAt(2, Instant.class));
          return traces.stream()
              .filter(t -> t.getLandscapeToken().equals(tok))
              .filter(t -> TimestampHelper.isAfterOrEqual(t.getStartTime(), from))
              .filter(t -> TimestampHelper.isBeforeOrEqual(t.getStartTime(), to))
              .map(t -> new HashSet<>(t.getSpanList()))
              .collect(Collectors.toList());
        });

    // Query for t2, t3
    final Collection<Trace> rangeQuery =
        this.service.getBetween(token, TimestampHelper.toInstant(t2.getStartTime()),
            TimestampHelper.toInstant(t3.getStartTime()));

    assertEquals(2, rangeQuery.size());

  }
}
