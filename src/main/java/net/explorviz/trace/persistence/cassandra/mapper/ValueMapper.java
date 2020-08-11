package net.explorviz.trace.persistence.cassandra.mapper;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.Map;

/**
 * Mapper to convert objects to cassandra row maps and vice versa.
 * @param <T> the java object type to map
 */
public interface ValueMapper<T> {

  /**
   * Converts the given object to a map where keys are cassandra database column names
   * and values are cassandra terms of corresponding attributes of the given object.
   * @param item the object to map
   * @return a map that can be inserted into a cassandra table
   */
  Map<String, Term> toMap(T item);

  /**
   * Reads the result row of a cassandra query into an object.
   * @param row the row as returned by a CSQL query
   * @return the object with attributes set according to the row
   */
  T fromRow(Row row);

}
