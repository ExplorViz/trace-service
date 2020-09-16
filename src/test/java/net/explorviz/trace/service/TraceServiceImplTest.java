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
import org.mockito.Mockito;

class TraceServiceImplTest {


  private TraceServiceImpl service;
  private TraceBuilder builder;
  private SpanRepository repo;

  @BeforeEach
  void setUp() {
    repo = Mockito.mock(SpanRepository.class);
    builder = new TraceBuilderImpl();
    service = new TraceServiceImpl(repo, builder);
  }

  @Test
  void getById() {
    int spanAmount = 10;
    Trace testTrace = TraceHelper.randomTrace(spanAmount);
    Mockito.when(repo.getSpans(testTrace.getLandscapeToken(), testTrace.getTraceId()))
        .thenReturn(Optional.of(testTrace.getSpanList()));

    Trace got =
        service.getById(testTrace.getLandscapeToken(), testTrace.getTraceId()).orElseThrow();

    assertEquals(testTrace, got);
  }

  @Test
  void getUnknownId() {
    int spanAmount = 10;
    Trace testTrace = TraceHelper.randomTrace(spanAmount);
    Mockito.when(repo.getSpans(testTrace.getLandscapeToken(), testTrace.getTraceId()))
        .thenReturn(Optional.of(testTrace.getSpanList()));
    Mockito.when(repo.getSpans(Mockito.anyString(), Mockito.anyString())).thenReturn(Optional.empty());

    Optional<Trace> gotUnknownToken = service.getById("sometoken", testTrace.getTraceId());
    Optional<Trace> gotUnknownTraceId = service.getById(testTrace.getLandscapeToken(), "someid");

    assertTrue(gotUnknownToken.isEmpty());
    assertTrue(gotUnknownTraceId.isEmpty());
  }

  @Test
  void getBetween() {
    int spanAmount = 10;
    // Create 4 traces of the same token in the temporal order
    // t4, t1, t2, t3
    String token = "token";
    Instant now = Instant.now();
    Trace t1 = TraceHelper.randomTrace(spanAmount);
    t1.setLandscapeToken(token);
    t1.setStartTime(TimestampHelper.toTimestamp(now.minus(10, ChronoUnit.SECONDS)));
    Trace t2 = TraceHelper.randomTrace(spanAmount);
    t2.setLandscapeToken(token);
    t2.setStartTime(TimestampHelper.toTimestamp(now.minus(5, ChronoUnit.SECONDS)));
    Trace t3 = TraceHelper.randomTrace(spanAmount);
    t3.setLandscapeToken(token);
    t3.setStartTime(TimestampHelper.toTimestamp(now));
    Trace t4 = TraceHelper.randomTrace(spanAmount);
    t4.setLandscapeToken(token);
    t4.setStartTime(TimestampHelper.toTimestamp(now.minus(20, ChronoUnit.SECONDS)));

    Set<Trace> traces = new HashSet<>(Arrays.asList(t1, t2, t3, t4));
    // Add random traces
    for (int i = 0; i < 0; i++) {
      traces.add(TraceHelper.randomTrace(spanAmount));
    }

    Mockito.when(repo.getAllInRange(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenAnswer(i -> {
          String tok = i.getArgumentAt(0, String.class);
          Timestamp from = TimestampHelper.toTimestamp(i.getArgumentAt(1, Instant.class));
          Timestamp to = TimestampHelper.toTimestamp(i.getArgumentAt(2, Instant.class));
          return traces.stream()
              .filter(t -> t.getLandscapeToken().equals(tok))
              .filter(t -> TimestampHelper.isAfterOrEqual(t.getStartTime(), from))
              .filter(t -> TimestampHelper.isBeforeOrEqual(t.getStartTime(), to))
              .map(t -> new HashSet<>(t.getSpanList()))
              .collect(Collectors.toList());
        });

    // Query for t2, t3
    Collection<Trace> rangeQuery =
        service.getBetween(token, TimestampHelper.toInstant(t2.getStartTime()),
            TimestampHelper.toInstant(t3.getStartTime()));

    assertEquals(2, rangeQuery.size());

  }
}
