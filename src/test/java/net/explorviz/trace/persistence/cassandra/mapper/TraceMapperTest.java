package net.explorviz.persistence.cassandra.mapper;


import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.DefaultLiteral;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import net.explorviz.persistence.cassandra.CassandraTest;
import net.explorviz.persistence.cassandra.DBHelper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TraceMapperTest extends CassandraTest {

  private TraceMapper mapper;


  @BeforeEach
  void setUp() {
    mapper = new TraceMapper(this.db.getCodecRegistry());
  }



  @Test
  void convertEmptySpans() {
    Instant iStart = Instant.now();
    Instant iEnd = iStart.plus(5, ChronoUnit.SECONDS).plus(123, ChronoUnit.NANOS);
    Trace trace = Trace.newBuilder()
        .setLandscapeToken("token")
        .setTraceId("trace_id")
        .setStartTime(new Timestamp(iStart.getEpochSecond(), iStart.getNano()))
        .setEndTime(new Timestamp(iEnd.getEpochSecond(), iEnd.getNano()))
        .setDuration(Duration.between(iStart, iEnd).toMillis())
        .setOverallRequestCount(1)
        .setTraceCount(1)
        .setSpanList(new ArrayList<>())
        .build();

    Map<String, Term> map = mapper.toMap(trace);
    String token = ((DefaultLiteral<String>) map.get(DBHelper.COL_TOKEN)).getValue();
    String traceId = ((DefaultLiteral<String>) map.get(DBHelper.COL_TRACE_ID)).getValue();
    Timestamp startTime = ((DefaultLiteral<Timestamp>) map.get(DBHelper.COL_START_TIME)).getValue();
    Timestamp endTime = ((DefaultLiteral<Timestamp>) map.get(DBHelper.COL_END_TIME)).getValue();
    List<SpanDynamic> spans =
        ((DefaultLiteral<List<SpanDynamic>>) map.get(DBHelper.COL_SPANS)).getValue();

    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getString(DBHelper.COL_TOKEN)).thenReturn(token);
    Mockito.when(row.getString(DBHelper.COL_TRACE_ID)).thenReturn(traceId);
    Mockito.when(row.get(DBHelper.COL_START_TIME, Timestamp.class)).thenReturn(startTime);
    Mockito.when(row.get(DBHelper.COL_END_TIME, Timestamp.class)).thenReturn(endTime);
    Mockito.when(row.getList(DBHelper.COL_SPANS, SpanDynamic.class)).thenReturn(spans);

    Trace got = mapper.fromRow(row);
    Assertions.assertEquals(trace, got, "Trace mapping failed");

  }

  @Test
  void convertWithSpans() {

    // Create list of randomSpans with completely random data
    List<SpanDynamic> randomSpans = new ArrayList<>();
    IntStream.range(0, 20).forEach(i -> randomSpans.add(randomSpan()));

    Instant iStart = Instant.now();
    Instant iEnd = iStart.plus(5, ChronoUnit.SECONDS).plus(123, ChronoUnit.NANOS);
    Trace trace = Trace.newBuilder()
        .setLandscapeToken("token")
        .setTraceId("trace_id")
        .setStartTime(new Timestamp(iStart.getEpochSecond(), iStart.getNano()))
        .setEndTime(new Timestamp(iEnd.getEpochSecond(), iEnd.getNano()))
        .setDuration(Duration.between(iStart, iEnd).toMillis())
        .setOverallRequestCount(1)
        .setTraceCount(1)
        .setSpanList(randomSpans)
        .build();

    Map<String, Term> map = mapper.toMap(trace);
    String token = ((DefaultLiteral<String>) map.get(DBHelper.COL_TOKEN)).getValue();
    String traceId = ((DefaultLiteral<String>) map.get(DBHelper.COL_TRACE_ID)).getValue();
    Timestamp startTime = ((DefaultLiteral<Timestamp>) map.get(DBHelper.COL_START_TIME)).getValue();
    Timestamp endTime = ((DefaultLiteral<Timestamp>) map.get(DBHelper.COL_END_TIME)).getValue();
    List<SpanDynamic> spans =
        ((DefaultLiteral<List<SpanDynamic>>) map.get(DBHelper.COL_SPANS)).getValue();

    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getString(DBHelper.COL_TOKEN)).thenReturn(token);
    Mockito.when(row.getString(DBHelper.COL_TRACE_ID)).thenReturn(traceId);
    Mockito.when(row.get(DBHelper.COL_START_TIME, Timestamp.class)).thenReturn(startTime);
    Mockito.when(row.get(DBHelper.COL_END_TIME, Timestamp.class)).thenReturn(endTime);
    Mockito.when(row.getList(DBHelper.COL_SPANS, SpanDynamic.class)).thenReturn(spans);

    Trace got = mapper.fromRow(row);
    Assertions.assertEquals(trace, got, "Trace mapping failed");

  }

  private SpanDynamic randomSpan() {

    return SpanDynamic.newBuilder()
        .setLandscapeToken(RandomStringUtils.random(32, true, true))
        .setStartTime(new Timestamp(RandomUtils.nextLong(), RandomUtils.nextInt()))
        .setEndTime(new Timestamp(RandomUtils.nextLong(), RandomUtils.nextInt()))
        .setTraceId(RandomStringUtils.random(6, true, true))
        .setParentSpanId(RandomStringUtils.random(8, true, true))
        .setSpanId(RandomStringUtils.random(8, true, true))
        .setHashCode(RandomStringUtils.random(256, true, true))
        .build();
  }

}
