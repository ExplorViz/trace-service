package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import java.util.List;

/**
 * Bean for a Trace.
 */
@Entity
public class Trace {

  @PartitionKey
  private String landscapeToken;

  @ClusteringColumn(1)
  private Timestamp startTime;

  @ClusteringColumn(2)
  private String traceId;

  private Timestamp endTime;
  private long duration;
  private int overallRequestCount;
  private int traceCount;

  // @Frozen not available in dependendy?
  private List<SpanDynamic> spanList;

  public Trace() {
    // for serialization
  }

  public Trace(final String landscapeToken, final String traceId, final Timestamp startTime,
      final Timestamp endTime,
      final long duration, final int overallRequestCount, final int traceCount,
      final List<SpanDynamic> spanList) {
    super();
    this.landscapeToken = landscapeToken;
    this.traceId = traceId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.duration = duration;
    this.overallRequestCount = overallRequestCount;
    this.traceCount = traceCount;
    this.spanList = spanList;
  }

  public String getLandscapeToken() {
    return this.landscapeToken;
  }

  public void setLandscapeToken(final String landscapeToken) {
    this.landscapeToken = landscapeToken;
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

  public long getDuration() {
    return this.duration;
  }

  public void setDuration(final long duration) {
    this.duration = duration;
  }

  public int getOverallRequestCount() {
    return this.overallRequestCount;
  }

  public void setOverallRequestCount(final int overallRequestCount) {
    this.overallRequestCount = overallRequestCount;
  }

  public int getTraceCount() {
    return this.traceCount;
  }

  public void setTraceCount(final int traceCount) {
    this.traceCount = traceCount;
  }

  public List<SpanDynamic> getSpanList() {
    return this.spanList;
  }

  public void setSpanList(final List<SpanDynamic> spanList) {
    this.spanList = spanList;
  }


}
