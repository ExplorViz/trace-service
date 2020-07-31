package net.explorviz.persistence.cassandra.codec;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Timestamp;

/**
 * Codec to convert spans UDT to {@link SpanDynamic}s.
 */
public class SpanCodec extends MappingCodec<UdtValue, SpanDynamic> {

  /**
   * Creates a new mapping codec providing support for {@link SpanDynamic} based on an existing codec for
   * {@code InnerT}.
   *
   * @param innerCodec    The inner codec to use to handle instances of InnerT; must not be null.
   */
  public SpanCodec(final TypeCodec<UdtValue> innerCodec) {
    super(innerCodec, GenericType.of(SpanDynamic.class));
  }

  @Override
  protected SpanDynamic innerToOuter( final UdtValue value) {
    return null;
  }


  @Override
  protected UdtValue outerToInner(final SpanDynamic value) {
    return null;
  }
}
