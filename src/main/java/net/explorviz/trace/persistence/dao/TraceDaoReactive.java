package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.quarkus.runtime.api.reactive.mapper.MutinyMappedReactiveResultSet;
import io.smallrye.mutiny.Uni;

/**
 * Datastax Dao for a {@link Trace}.
 */
@Dao
public interface TraceDaoReactive {

  @Insert
  Uni<Void> insertAsync(Trace trace);

  @Update
  Uni<Void> updateAsync(Trace trace);

  @Select
  MutinyMappedReactiveResultSet<Trace> findByIdAsync(String id);
}

