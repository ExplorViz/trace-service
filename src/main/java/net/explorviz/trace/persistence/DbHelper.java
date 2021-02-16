package net.explorviz.trace.persistence;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndex;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for accessing the Cassandra database. Next to providing access to the CqlSession,
 * this class contain utility methods to initialize the database. This database's state by default
 * is uninitialized. To create the necessary keyspace and tables, call {@link #initialize()} prior
 * to using it. In essence this creates the the table `spans` with the following schema
 *
 * <pre>
 *   LandscapeToken* | Timestamp' | TraceId' | Set&lt;SpanDynamic&gt;
 * </pre>
 */
@Singleton
public class DbHelper {

  private static final String KEYSPACE_NAME = "explorviz";
  private static final String TABLE_TRACES = "trace"; // NOCS

  private static final String TYPE_TIMESTAMP = "explorviz_timestamp";
  private static final String TYPE_SPAN = "span";

  private static final String COL_TOKEN = "landscape_token";
  private static final String COL_TRACE_ID = "trace_id";

  private static final String COL_TIMESTAMP_SECONDS = "seconds";
  private static final String COL_TIMESTAMP_NANO = "nano_adjust";

  private static final String COL_TRACE_DURATION = "duration";
  private static final String COL_TRACE_REQUESTS = "overall_request_count";
  private static final String COL_TRACE_COUNT = "trace_count";

  private static final String COL_TRACE_START_TIMESTAMP = "start_time";
  private static final String COL_TRACE_END_TIMESTAMP = "end_time";

  private static final String COL_SPAN_ID = "span_id";
  private static final String COL_SPAN_TRACE_ID = "trace_id";
  private static final String COL_SPAN_PARENT_ID = "parent_span_id";
  private static final String COL_SPAN_START_TIME = "start_time";
  private static final String COL_SPAN_END_TIME = "end_time";
  private static final String COL_SPAN_HASH = "hash_code";


  public static final String COL_TRACE_SPANS = "span_list"; // NOCS

  private static final Logger LOGGER = LoggerFactory.getLogger(DbHelper.class);


  private final CqlSession dbSession;

  @Inject
  public DbHelper(final CqlSession session) {
    this.dbSession = session;
  }

  public CqlSession getSession() {
    return this.dbSession;
  }

  /**
   * Initializes the database by creating necessary schemata. This is a no-op if the database is
   * already initialized.
   */
  public void initialize() {
    this.createKeySpace();
    this.createTables();
  }

  /**
   * Creates a keyspace name "explorviz". No-op if this keyspace already exists.
   */
  private void createKeySpace() {
    LOGGER.info("Trying to create Keyspace");
    final CreateKeyspace createKs = SchemaBuilder
        .createKeyspace(KEYSPACE_NAME)
        .ifNotExists()
        .withSimpleStrategy(1)
        .withDurableWrites(true);
    this.dbSession.execute(createKs.build());
    LOGGER.info("Created Keyspace");
  }

  /**
   * Creates the table "traces" which holds all {@link net.explorviz.avro.Trace} objects. No-op if
   * this table already exists.
   */
  private void createTables() {

    final CreateType createTimestampUdt = SchemaBuilder
        .createType(KEYSPACE_NAME, TYPE_TIMESTAMP)
        .ifNotExists()
        .withField(COL_TIMESTAMP_SECONDS, DataTypes.BIGINT)
        .withField(COL_TIMESTAMP_NANO, DataTypes.INT);


    final CreateType createSpanUdt = SchemaBuilder
        .createType(KEYSPACE_NAME, TYPE_SPAN)
        .ifNotExists()
        .withField(COL_TOKEN, DataTypes.TEXT)
        .withField(COL_SPAN_TRACE_ID, DataTypes.TEXT)
        .withField(COL_SPAN_ID, DataTypes.TEXT)
        .withField(COL_SPAN_PARENT_ID, DataTypes.TEXT)
        .withField(COL_SPAN_START_TIME, SchemaBuilder.udt(TYPE_TIMESTAMP, true))
        .withField(COL_SPAN_END_TIME, SchemaBuilder.udt(TYPE_TIMESTAMP, true))
        .withField(COL_SPAN_HASH, DataTypes.TEXT);


    final CreateTable createTraceTable = SchemaBuilder
        .createTable(KEYSPACE_NAME, TABLE_TRACES)
        .ifNotExists()
        .withPartitionKey(COL_TOKEN, DataTypes.TEXT)
        .withClusteringColumn(COL_TRACE_START_TIMESTAMP, SchemaBuilder.udt(TYPE_TIMESTAMP, true))
        .withClusteringColumn(COL_TRACE_ID, DataTypes.TEXT)
        .withColumn(COL_TRACE_END_TIMESTAMP, SchemaBuilder.udt(TYPE_TIMESTAMP, true))
        .withColumn(COL_TRACE_DURATION, DataTypes.BIGINT)
        .withColumn(COL_TRACE_REQUESTS, DataTypes.INT)
        .withColumn(COL_TRACE_COUNT, DataTypes.INT)
        .withColumn(COL_TRACE_SPANS, DataTypes.listOf(SchemaBuilder.udt(TYPE_SPAN, true), true));

    // Create index on start time for efficient range queries
    final CreateIndex createTimestampIndex = SchemaBuilder.createIndex("timestamp_index")
        .ifNotExists()
        .onTable(KEYSPACE_NAME, TABLE_TRACES)
        .andColumn(COL_TRACE_START_TIMESTAMP);

    this.dbSession.execute(createTimestampUdt.asCql());
    this.dbSession.execute(createSpanUdt.asCql());
    this.dbSession.execute(createTraceTable.asCql());
    this.dbSession.execute(createTimestampIndex.asCql());

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Created trace table and associated types");
    }

  }

}
