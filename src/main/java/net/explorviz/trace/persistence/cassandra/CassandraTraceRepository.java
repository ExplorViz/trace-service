package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.Map;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.Trace;
import net.explorviz.trace.persistence.PersistingException;
import net.explorviz.trace.persistence.TraceRepository;
import net.explorviz.trace.persistence.cassandra.mapper.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra-backed repository to access and save {@link net.explorviz.avro.Trace} entities.
 */
@ApplicationScoped
public class CassandraTraceRepository implements TraceRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTraceRepository.class);

  private final DBHelper db;
  private final ValueMapper<Trace> mapper;

  /**
   * Create a new repository for accessing {@link Trace} object.
   *
   * @param db the backing Casandra db
   */
  @Inject
  public CassandraTraceRepository(final DBHelper db, final ValueMapper<Trace> mapper) {
    this.db = db;
    db.initialize();
    this.mapper = mapper;
  }



  @Override
  public void insert(final Trace item) throws PersistingException {



    final Map<String, Term> values = this.mapper.toMap(item);
    final SimpleStatement insertStmt =
        QueryBuilder.insertInto(DBHelper.KEYSPACE_NAME, DBHelper.TRACES_TABLE_NAME)
            .values(values)
            .build();
    try {
      this.db.getSession().execute(insertStmt);
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Inserted new record for id {}", item.getTraceId());
      }
    } catch (final AllNodesFailedException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Failed to insert new record: Database unreachable");
      }
      throw new PersistingException(e);
    } catch (QueryExecutionException | QueryValidationException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Failed to insert new record: {0}", e.getCause());
      }
      throw new PersistingException(e);
    }
  }

  @Override
  public void update(final Trace trace) throws PersistingException {

  }

  @Override
  public Optional<Trace> getTrace(final String landscapeToken, final String traceId) {
    return Optional.empty();
  }


}
