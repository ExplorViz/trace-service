package net.explorviz.trace.persistence.cassandra.codecs;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.trace.persistence.cassandra.DbHelper;

/**
 * Codec to convert spans UDT to {@link SpanDynamic}s.
 */
public class SpanCodec extends MappingCodec<UdtValue, SpanDynamic> {

  private final TimestampCodec timestampTypeCodec;

  /**
   * Creates a new mapping codec providing support for {@link SpanDynamic} based on an existing
   * codec for {@code InnerT}.
   *
   * @param innerCodec The inner codec to use to handle instances of InnerT; must not be null.
   */
  public SpanCodec(final TypeCodec<UdtValue> innerCodec,
      final TimestampCodec timestampCodec) {
    super(innerCodec, GenericType.of(SpanDynamic.class));
    this.timestampTypeCodec = timestampCodec;
  }

  @Override
  protected SpanDynamic innerToOuter(final UdtValue value) {


    final Timestamp start =
        this.timestampTypeCodec.innerToOuter(value.getUdtValue(DbHelper.COL_SPAN_START_TIME));
    final Timestamp end =
        this.timestampTypeCodec.innerToOuter(value.getUdtValue(DbHelper.COL_SPAN_END_TIME));

    return SpanDynamic.newBuilder()
        .setTraceId(value.getString(DbHelper.COL_SPAN_TRACE_ID))
        .setSpanId(value.getString(DbHelper.COL_SPAN_ID))
        .setParentSpanId(value.getString(DbHelper.COL_SPAN_PARENT_ID))
        .setLandscapeToken("") // Remove this information as its given in the trace
        .setStartTime(start)
        .setEndTime(end)
        .setHashCode(value.getString(DbHelper.COL_SPAN_HASH))
        .build();
  }



  @Override
  public UdtValue outerToInner(final SpanDynamic value) {

    final UdtValue udtValue = ((UserDefinedType) this.getCqlType()).newValue();


    final UdtValue start = this.timestampTypeCodec.outerToInner(value.getStartTime());
    final UdtValue end = this.timestampTypeCodec.outerToInner(value.getEndTime());

    udtValue.setString(DbHelper.COL_SPAN_TRACE_ID, value.getTraceId());
    udtValue.setString(DbHelper.COL_SPAN_ID, value.getSpanId());
    udtValue.setString(DbHelper.COL_SPAN_PARENT_ID, value.getParentSpanId());
    udtValue.setUdtValue(DbHelper.COL_SPAN_START_TIME, start);
    udtValue.setUdtValue(DbHelper.COL_SPAN_END_TIME, end);
    udtValue.setString(DbHelper.COL_SPAN_HASH, value.getHashCode());

    return udtValue;
  }
}
