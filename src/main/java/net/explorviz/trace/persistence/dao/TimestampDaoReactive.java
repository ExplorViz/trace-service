package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.quarkus.runtime.api.reactive.mapper.MutinyMappedReactiveResultSet;
import io.smallrye.mutiny.Uni;

/**
 * Datastax Dao for a {@link Timestamp}.
 */
@Dao
public interface TimestampDaoReactive {

  @Update
  Uni<Void> updateAsync(Timestamp timestamp);

  @Select
  MutinyMappedReactiveResultSet<Timestamp> findByIdAsync(int id);
}
