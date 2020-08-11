package net.explorviz.persistence.cassandra.mapper;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import net.explorviz.avro.Timestamp;
import net.explorviz.persistence.cassandra.DBHelper;

/**
 * Codec to convert timestamp UDT to {@link Timestamp}s.
 */
public class TimestampCodec extends MappingCodec<UdtValue, Timestamp> {

  /**
   * Creates a new mapping codec providing support for {@link Timestamp} based on an existing codec for
   * {@code InnerT}.
   *
   * @param innerCodec    The inner codec to use to handle instances of InnerT; must not be null.
   */
  public TimestampCodec(final TypeCodec<UdtValue> innerCodec) {
    super(innerCodec, GenericType.of(Timestamp.class));
  }

  @Override
  protected Timestamp innerToOuter( final UdtValue value) {
    Long seconds = value.getLong(DBHelper.COL_TIMESTAMP_SECONDS);
    Integer nanoAdjust = value.getInt(DBHelper.COL_TIMESTAMP_NANO);
    return new Timestamp(seconds, nanoAdjust);
  }


  @Override
  protected UdtValue outerToInner(final Timestamp value) {
    UdtValue udtValue = ((UserDefinedType) getCqlType()).newValue();
    udtValue.setLong(DBHelper.COL_TIMESTAMP_SECONDS, value.getSeconds());
    udtValue.setInt(DBHelper.COL_TIMESTAMP_NANO, value.getNanoAdjust());
    return udtValue;
  }


}
