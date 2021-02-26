package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import java.util.List;
import java.util.Objects;

/**
 * Bean for a Trace.
 */
@Entity
public class Trace {

  @PartitionKey
  private String landscapeToken;

  @ClusteringColumn(1)
  private long startTime;

  @ClusteringColumn(2)
  private String traceId;

  private long endTime;
  private long duration;
  private int overallRequestCount;
  private int traceCount;

  // @Frozen not available in dependendy?
  private List<SpanDynamic> spanList;

  public Trace() {
    // for serialization
  }

  public Trace(final String landscapeToken, final String traceId, final long startTime,
      final long endTime,
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

  public long getStartTime() {
    return this.startTime;
  }

  public void setStartTime(final long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return this.endTime;
  }

  public void setEndTime(final long endTime) {
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

  @Override
  public int hashCode() {
    return Objects.hash(this.duration, this.endTime, this.landscapeToken, this.overallRequestCount,
        this.spanList, this.startTime, this.traceCount, this.traceId);
  }

  @Override // NOCS
  public boolean equals(final Object obj) { // NOPMD
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    final Trace other = (Trace) obj;
    if (this.duration != other.duration) {
      return false;
    }
    if (this.endTime != other.endTime) {
      return false;
    }
    if (this.landscapeToken == null) {
      if (other.landscapeToken != null) {
        return false;
      }
    } else if (!this.landscapeToken.equals(other.landscapeToken)) {
      return false;
    }
    if (this.overallRequestCount != other.overallRequestCount) {
      return false;
    }
    if (this.spanList == null) {
      if (other.spanList != null) {
        return false;
      }
    } else if (!this.spanList.equals(other.spanList)) {
      return false;
    }
    if (this.startTime != other.startTime) {
      return false;
    }
    if (this.traceCount != other.traceCount) {
      return false;
    }
    if (this.traceId == null) {
      if (other.traceId != null) {
        return false;
      }
    } else if (!this.traceId.equals(other.traceId)) {
      return false;
    }
    return true;
  }



}
