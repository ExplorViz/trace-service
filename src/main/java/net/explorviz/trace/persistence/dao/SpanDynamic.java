package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

/**
 * Bean for dynamic Span data.
 */
@Entity
public class SpanDynamic {

  @PartitionKey
  private String landscapeToken;

  @ClusteringColumn(1)
  private String traceId;

  @ClusteringColumn(2)
  private String spanId;
  private String parentSpanId;

  @ClusteringColumn(3) // NOCS
  private Timestamp startTime;

  private Timestamp endTime;
  private String hashCode;

  public SpanDynamic() {
    // for serialization
  }

  public SpanDynamic(final String landscapeToken, final String spanId, final String parentSpanId,
      final String traceId,
      final Timestamp startTime, final Timestamp endTime, final String hashCode) {
    super();
    this.landscapeToken = landscapeToken;
    this.spanId = spanId;
    this.parentSpanId = parentSpanId;
    this.traceId = traceId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.hashCode = hashCode;
  }



  public String getLandscapeToken() {
    return this.landscapeToken;
  }

  public void setLandscapeToken(final String landscapeToken) {
    this.landscapeToken = landscapeToken;
  }

  public String getSpanId() {
    return this.spanId;
  }

  public void setSpanId(final String spanId) {
    this.spanId = spanId;
  }

  public String getParentSpanId() {
    return this.parentSpanId;
  }

  public void setParentSpanId(final String parentSpanId) {
    this.parentSpanId = parentSpanId;
  }

  public String getTraceId() {
    return this.traceId;
  }

  public void setTraceId(final String traceId) {
    this.traceId = traceId;
  }

  public Timestamp getStartTime() {
    return this.startTime;
  }

  public void setStartTime(final Timestamp startTime) {
    this.startTime = startTime;
  }

  public Timestamp getEndTime() {
    return this.endTime;
  }

  public void setEndTime(final Timestamp endTime) {
    this.endTime = endTime;
  }

  public String getHashCode() {
    return this.hashCode;
  }

  public void setHashCode(final String hashCode) {
    this.hashCode = hashCode;
  }


}
