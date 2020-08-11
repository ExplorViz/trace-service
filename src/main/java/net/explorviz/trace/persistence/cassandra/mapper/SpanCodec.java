package net.explorviz.persistence.cassandra.mapper;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;
import net.explorviz.persistence.cassandra.DBHelper;

/**
 * Codec to convert spans UDT to {@link SpanDynamic}s.
 */
public class SpanCodec extends MappingCodec<UdtValue, SpanDynamic> {

  private final MappingCodec<UdtValue, Timestamp> timestampTypeCodec;

  /**
   * Creates a new mapping codec providing support for {@link SpanDynamic} based on an existing
   * codec for
   * {@code InnerT}.
   *
   * @param innerCodec The inner codec to use to handle instances of InnerT; must not be null.
   */
  public SpanCodec(final TypeCodec<UdtValue> innerCodec,
                   final MappingCodec<UdtValue, Timestamp> timestampCodec) {
    super(innerCodec, GenericType.of(SpanDynamic.class));
    this.timestampTypeCodec = timestampCodec;
  }

  @Override
  protected SpanDynamic innerToOuter(final UdtValue value) {
    Timestamp start = timestampTypeCodec
        .decode(value.getByteBuffer(DBHelper.COL_START_TIME), ProtocolVersion.DEFAULT);
    Timestamp end = timestampTypeCodec
        .decode(value.getByteBuffer(DBHelper.COL_END_TIME), ProtocolVersion.DEFAULT);

    SpanDynamic.newBuilder()
        .setTraceId(value.getString(DBHelper.COL_SPAN_TRACE_ID))
        .setSpanId(value.getString(DBHelper.COL_SPAN_ID))
        .setParentSpanId(value.getString(DBHelper.COL_SPAN_PARENT_ID))
        .setLandscapeToken("")
        .setStartTime(start)
        .setEndTime(end)
        .setHashCode(value.getString(DBHelper.COL_SPAN_HASH));
    return null;
  }


  @Override
  protected UdtValue outerToInner(final SpanDynamic value) {
    UdtValue udtValue = ((UserDefinedType) getCqlType()).newValue();

    ByteBuffer start = timestampTypeCodec.encode(value.getStartTime(), ProtocolVersion.DEFAULT);
    ByteBuffer end = timestampTypeCodec.encode(value.getEndTime(), ProtocolVersion.DEFAULT);

    udtValue.setString(DBHelper.COL_SPAN_TRACE_ID, value.getTraceId());
    udtValue.setString(DBHelper.COL_SPAN_ID, value.getSpanId());
    udtValue.setString(DBHelper.COL_SPAN_PARENT_ID, value.getParentSpanId());
    udtValue.setByteBuffer(DBHelper.COL_START_TIME, start);
    udtValue.setByteBuffer(DBHelper.COL_END_TIME, end);
    udtValue.setString(DBHelper.COL_SPAN_HASH, value.getHashCode());

    return udtValue;
  }
}
