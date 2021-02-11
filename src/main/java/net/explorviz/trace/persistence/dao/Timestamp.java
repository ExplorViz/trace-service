package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

/**
 * Bean for dynamic Span data.
 */
@Entity
public class Timestamp {

  @PartitionKey
  private int shardId;

  @ClusteringColumn(1)
  private long seconds;

  @ClusteringColumn(2)
  private int nanoAdjust;

  public Timestamp() {
    // for serialization
  }

  public Timestamp(final int shardId, final long seconds, final int nanoAdjust) {
    super();
    this.shardId = shardId;
    this.seconds = seconds;
    this.nanoAdjust = nanoAdjust;
  }

  public int getShardId() {
    return shardId;
  }

  public void setShardId(final int shardId) {
    this.shardId = shardId;
  }

  public long getSeconds() {
    return seconds;
  }

  public void setSeconds(final long seconds) {
    this.seconds = seconds;
  }

  public int getNanoAdjust() {
    return nanoAdjust;
  }

  public void setNanoAdjust(final int nanoAdjust) {
    this.nanoAdjust = nanoAdjust;
  }



}
