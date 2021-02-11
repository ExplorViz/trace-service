package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

/**
 * Datastax Dao Mapper for {@link Timestamp}.
 */
@Mapper
public interface TimestampMapper {

  @DaoFactory
  TimestampDaoReactive timestampDaoReactive();

}
