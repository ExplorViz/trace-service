package net.explorviz.persistence.cassandra.codec;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.Map;

public interface ValueMapper<T> {

  Map<String, Term> toMap(T item);

  T fromRow(Row row);

}
