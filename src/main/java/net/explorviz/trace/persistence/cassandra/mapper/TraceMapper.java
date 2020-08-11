package net.explorviz.trace.persistence.cassandra.mapper;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.avro.Trace;
import net.explorviz.trace.persistence.cassandra.DBHelper;

/**
 * Maps {@link Trace}s to cassandra rows and back.
 */
@ApplicationScoped
public class TraceMapper implements ValueMapper<Trace> {

  private final CodecRegistry codecRegistry;

  public TraceMapper(final DBHelper dbHelper) {
    this.codecRegistry = dbHelper.getCodecRegistry();
  }

  @Override
  public Map<String, Term> toMap(final Trace trace) {
    Map<String, Term> map = new HashMap<>();

    QueryBuilder.literal(Set.of(trace.getSpanList()), codecRegistry);
    map.put(DBHelper.COL_TOKEN, QueryBuilder.literal(trace.getLandscapeToken()));
    map.put(DBHelper.COL_TRACE_ID, QueryBuilder.literal(trace.getTraceId()));
    map.put(DBHelper.COL_START_TIME, QueryBuilder.literal(trace.getStartTime(), codecRegistry));
    map.put(DBHelper.COL_END_TIME, QueryBuilder.literal(trace.getEndTime(), codecRegistry));
    Set<SpanDynamic> sds = Set.copyOf(trace.getSpanList());
    map.put(DBHelper.COL_SPANS, QueryBuilder.literal(sds, codecRegistry));
    return map;

  }

  @Override
  public Trace fromRow(final Row row) {
    Timestamp start = row.get(DBHelper.COL_START_TIME, Timestamp.class);
    Timestamp end = row.get(DBHelper.COL_END_TIME, Timestamp.class);

    Set<SpanDynamic> spanSet = row.getSet(DBHelper.COL_SPANS, SpanDynamic.class);
    List<SpanDynamic> spanList = new ArrayList<>(spanSet.size());
    spanList.addAll(spanSet);

    String token = row.getString(DBHelper.COL_TOKEN);
    // Set the token for each span as they are not persisted
    spanList.forEach(s -> s.setLandscapeToken(token));

    return Trace.newBuilder()
        .setLandscapeToken(token)
        .setTraceId(row.getString(DBHelper.COL_TRACE_ID))
        .setStartTime(start)
        .setEndTime(end)
        .setDuration(calculateDuration(start, end))
        .setSpanList(spanList)
        .build();
  }

  private long calculateDuration(Timestamp start, Timestamp end) {
    Instant iStart = Instant.ofEpochSecond(start.getSeconds(), start.getNanoAdjust());
    Instant iEnd = Instant.ofEpochSecond(end.getSeconds(), end.getNanoAdjust());
    return Duration.between(iStart, iEnd).toMillis();
  }
}
