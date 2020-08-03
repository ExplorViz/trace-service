package net.explorviz.persistence.cassandra.mapper;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import jnr.ffi.annotations.In;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import net.explorviz.persistence.cassandra.DBHelper;

/**
 * Maps {@link Trace}s to cassandra rows and back.
 */
@ApplicationScoped
public class TraceMapper implements ValueMapper<Trace> {

  private final CodecRegistry codecRegistry;

  @Inject
  public TraceMapper(final CodecRegistry codecRegistry) {
    this.codecRegistry = codecRegistry;
  }

  @Override
  public Map<String, Term> toMap(final Trace trace) {
    Map<String, Term> map = new HashMap<>();

    map.put(DBHelper.COL_TOKEN, QueryBuilder.literal(trace.getLandscapeToken()));
    map.put(DBHelper.COL_TRACE_ID, QueryBuilder.literal(trace.getTraceId()));
    map.put(DBHelper.COL_START_TIME, QueryBuilder.literal(trace.getStartTime(), codecRegistry));
    map.put(DBHelper.COL_END_TIME, QueryBuilder.literal(trace.getEndTime(), codecRegistry));
    map.put(DBHelper.COL_SPANS, QueryBuilder.literal(trace.getSpanList(), codecRegistry));
    return map;

  }

  @Override
  public Trace fromRow(final Row row) {
    Timestamp start = row.get(DBHelper.COL_START_TIME, Timestamp.class);
    Timestamp end = row.get(DBHelper.COL_END_TIME, Timestamp.class);

    return Trace.newBuilder()
        .setLandscapeToken(row.getString(DBHelper.COL_TOKEN))
        .setTraceId(row.getString(DBHelper.COL_TRACE_ID))
        .setStartTime(start)
        .setEndTime(end)
        .setDuration(calculateDuration(start, end))
        .setSpanList(row.getList(DBHelper.COL_SPANS, SpanDynamic.class))
        .build();
  }

  private long calculateDuration(Timestamp start, Timestamp end) {
    Instant iStart = Instant.ofEpochSecond(start.getSeconds(), start.getNanoAdjust());
    Instant iEnd = Instant.ofEpochSecond(end.getSeconds(), end.getNanoAdjust());
    return Duration.between(iStart, iEnd).toMillis();
  }
}
