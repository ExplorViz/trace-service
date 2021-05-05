package net.explorviz.trace.kafka;

import java.time.Instant;
import net.explorviz.avro.SpanDynamic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Timestamp extractor for spans. Uses the start time of a spans as the record's timestamp used for
 * windowing.
 */
public class SpanTimestampKafkaExtractor implements TimestampExtractor {

  public SpanTimestampKafkaExtractor() { // NOPMD
    // nothing to do, necessary for native image
  }

  @Override
  public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
    final SpanDynamic span = (SpanDynamic) record.value();

    if (span != null) {
      // timestamp = Duration.ofNanos(span.getStartTime()).toMillis();
      // timestamp = Instant.ofEpochMilli(span.getStartTime()).toEpochMilli();
      return Instant
          .ofEpochSecond(span.getStartTime().getSeconds(), span.getStartTime().getNanoAdjust())
          .toEpochMilli();
    }

    // Invalid timestamp! Attempt to estimate a new timestamp,
    // otherwise fall back to wall-clock time (processing-time).
    if (previousTimestamp >= 0) {
      return previousTimestamp;
    } else {
      return System.currentTimeMillis();
    }


  }


}
