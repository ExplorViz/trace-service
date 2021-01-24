package net.explorviz.trace.persistence.cassandra.codecs;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import net.explorviz.avro.Timestamp;
import net.explorviz.trace.persistence.cassandra.DbHelper;

/**
 * Codec to convert timestamp UDT to {@link Timestamp}s.
 */
public class TimestampCodec extends MappingCodec<UdtValue, Timestamp> {

  /**
   * Creates a new mapping codec providing support for {@link Timestamp} based on an existing codec
   * for {@code InnerT}.
   *
   * @param innerCodec The inner codec to use to handle instances of InnerT; must not be null.
   */
  public TimestampCodec(final TypeCodec<UdtValue> innerCodec) {
    super(innerCodec, GenericType.of(Timestamp.class));
  }

  @Override
  public Timestamp innerToOuter(final UdtValue value) {
    final Long seconds = value.getLong(DbHelper.COL_TIMESTAMP_SECONDS);
    final Integer nanoAdjust = value.getInt(DbHelper.COL_TIMESTAMP_NANO);
    return new Timestamp(seconds, nanoAdjust);
  }


  @Override
  public UdtValue outerToInner(final Timestamp value) {
    final UdtValue udtValue = ((UserDefinedType) this.getCqlType()).newValue();
    udtValue.setLong(DbHelper.COL_TIMESTAMP_SECONDS, value.getSeconds());
    udtValue.setInt(DbHelper.COL_TIMESTAMP_NANO, value.getNanoAdjust());
    return udtValue;
  }


}
