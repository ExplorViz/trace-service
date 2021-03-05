package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.quarkus.runtime.api.reactive.mapper.MutinyMappedReactiveResultSet;
import io.smallrye.mutiny.Uni;

/**
 * Datastax Dao for a {@link SpanDynamic}.
 */
@Dao
public interface SpanDynamicDaoReactive {

  @Update
  Uni<Void> updateAsync(SpanDynamic spanDynamic);

  @Select
  MutinyMappedReactiveResultSet<SpanDynamic> findByIdAsync(String id);
}

