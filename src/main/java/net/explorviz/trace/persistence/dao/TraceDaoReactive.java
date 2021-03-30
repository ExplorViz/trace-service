package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.quarkus.runtime.api.reactive.mapper.MutinyMappedReactiveResultSet;
import io.smallrye.mutiny.Uni;

/**
 * Datastax Dao for a {@link Trace}.
 */
@Dao
public interface TraceDaoReactive {

  @Insert
  Uni<Void> insertAsync(Trace trace);

  @Select(customWhereClause = "landscape_token = :id")
  MutinyMappedReactiveResultSet<Trace> getAllAsync(String id);

  @Select(
      customWhereClause = "landscape_token = :id and start_time >= :startTime and "
          + "start_time <= :endTime")
  MutinyMappedReactiveResultSet<Trace> getByStartTimeAndEndTime(String id, long startTime,
      long endTime);

  @Select(customWhereClause = "landscape_token = :id and trace_id = :traceId")
  MutinyMappedReactiveResultSet<Trace> getByTraceId(String id, String traceId);
}

