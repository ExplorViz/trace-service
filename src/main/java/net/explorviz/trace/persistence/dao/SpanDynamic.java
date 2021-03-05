package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import java.util.Objects;

/**
 * Bean for dynamic Span data.
 */
@Entity
public class SpanDynamic {

  private String landscapeToken;

  private String traceId;

  private String spanId;
  private String parentSpanId;

  private long startTime;

  private long endTime;
  private String hashCode; // NOPMD

  public SpanDynamic() {
    // for serialization
  }

  public SpanDynamic(final String landscapeToken, final String spanId, final String parentSpanId,
      final String traceId,
      final long startTime, final long endTime, final String hashCode) {
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

  public String getHashCode() {
    return this.hashCode;
  }

  public void setHashCode(final String hashCode) {
    this.hashCode = hashCode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.endTime, this.hashCode, this.landscapeToken, this.parentSpanId,
        this.spanId,
        this.startTime, this.traceId);
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
    final SpanDynamic other = (SpanDynamic) obj;
    if (this.endTime != other.endTime) {
      return false;
    }
    if (this.hashCode == null) {
      if (other.hashCode != null) {
        return false;
      }
    } else if (!this.hashCode.equals(other.hashCode)) {
      return false;
    }
    if (this.landscapeToken == null) {
      if (other.landscapeToken != null) {
        return false;
      }
    } else if (!this.landscapeToken.equals(other.landscapeToken)) {
      return false;
    }
    if (this.parentSpanId == null) {
      if (other.parentSpanId != null) {
        return false;
      }
    } else if (!this.parentSpanId.equals(other.parentSpanId)) {
      return false;
    }
    if (this.spanId == null) {
      if (other.spanId != null) {
        return false;
      }
    } else if (!this.spanId.equals(other.spanId)) {
      return false;
    }
    if (this.startTime != other.startTime) {
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
