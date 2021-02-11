package net.explorviz.trace.persistence.dao;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

/**
 * Datastax Dao Mapper for {@link SpanDynamic}.
 */
@Mapper
public interface SpanDynamicMapper {

  @DaoFactory
  SpanDynamicDaoReactive spanDynamicDaoReactive();

}
