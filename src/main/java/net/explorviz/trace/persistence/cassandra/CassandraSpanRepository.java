package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import java.util.Optional;
import java.util.Set;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;
import net.explorviz.trace.persistence.PersistingException;
import net.explorviz.trace.persistence.SpanRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra-backed repository to access and save {@link net.explorviz.avro.Trace} entities.
 */
@ApplicationScoped
public class CassandraSpanRepository implements SpanRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSpanRepository.class);

  private final DBHelper db;

  /**
   * Create a new repository for accessing {@link Trace} object.
   *
   * @param db the backing Casandra db
   */
  @Inject
  public CassandraSpanRepository(final DBHelper db) {
    this.db = db;
    db.initialize();
  }



  @Override
  public void insert(SpanDynamic span) throws PersistingException {


    try {
      // Try to append span to the set of spans corresponding the trace id and token
      // This fails if the row does not exists
      if (appendSpan(span)) {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("Inserted new span for landscape with token {} (trace id {})",
              span.getLandscapeToken(), span.getTraceId());
        }
      } else {
        // Row for this trace id and token does not exist, create it with the single span
        createNew(span);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("Created new trace with single span for"
                  + " landscape with token {} (trace id {})",
              span.getLandscapeToken(), span.getTraceId());
        }
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


  /**
   * Creates a new entry.
   *
   * @param span the span to add
   */
  private void createNew(SpanDynamic span) {
    final SimpleStatement insertStmt =
        QueryBuilder.insertInto(DBHelper.KEYSPACE_NAME, DBHelper.TABLE_SPANS)
            .value(DBHelper.COL_TOKEN, QueryBuilder.literal(span.getLandscapeToken()))
            .value(DBHelper.COL_TIMESTAMP, QueryBuilder.toTimestamp(QueryBuilder.now()))
            .value(DBHelper.COL_TRACE_ID, QueryBuilder.literal(span.getTraceId()))
            .value(DBHelper.COL_SPANS, QueryBuilder.literal(Set.of(span), db.getCodecRegistry()))
            .build();
    this.db.getSession().execute(insertStmt);
  }

  /**
   * Appends a span to the (existing) set of all spans of the very trace id and landscape token
   *
   * @param span span to append to set of spans with the same trace id
   * @return true iff the update was successful, false if the corresponding row does not exists
   */
  private boolean appendSpan(SpanDynamic span) {
    final SimpleStatement updateStmt =
        QueryBuilder.update(DBHelper.KEYSPACE_NAME, DBHelper.TABLE_SPANS)
            .appendSetElement(DBHelper.COL_SPANS, QueryBuilder.literal(span, db.getCodecRegistry()))
            .whereColumn(DBHelper.COL_TOKEN)
            .isEqualTo(QueryBuilder.literal(span.getLandscapeToken()))
            .whereColumn(DBHelper.COL_TRACE_ID).isEqualTo(QueryBuilder.literal(span.getTraceId()))
            .ifExists()
            .build();

    return this.db.getSession().execute(updateStmt).wasApplied();

  }

  @Override
  public Optional<Set<SpanDynamic>> getSpans(final String landscapeToken, final String traceId) {
    return Optional.empty();
  }


}
