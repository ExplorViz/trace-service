package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.Entity;

/**
 * Bean for dynamic Span data.
 */
@Entity
public class Timestamp {

  private long seconds;

  private int nanoAdjust;

  public Timestamp() {
    // for serialization
  }

  public Timestamp(final long seconds, final int nanoAdjust) {
    super();
    this.seconds = seconds;
    this.nanoAdjust = nanoAdjust;
  }

  public long getSeconds() {
    return this.seconds;
  }

  public void setSeconds(final long seconds) {
    this.seconds = seconds;
  }

  public int getNanoAdjust() {
    return this.nanoAdjust;
  }

  public void setNanoAdjust(final int nanoAdjust) {
    this.nanoAdjust = nanoAdjust;
  }



}
