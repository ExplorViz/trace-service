package net.explorviz.trace.persistence.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndex;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.explorviz.trace.persistence.cassandra.mapper.SpanCodec;
import net.explorviz.trace.persistence.cassandra.mapper.TimestampCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for accessing the Cassandra database. Next to providing access to the CqlSession,
 * this class contain utility methods to initialize the database. This database's state by default
 * is uninitialized. To create the necessary keyspace and tables, call {@link #initialize()} prior
 * to using it.
 */
@Singleton
public class DBHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBHelper.class);

  public static final String KEYSPACE_NAME = "explorviz";
  public static final String TRACES_TABLE_NAME = "traces";


  public static final String TYPE_TIMESTAMP = "ctimestamp";
  public static final String TYPE_SPAN = "span";



  public static final String COL_TOKEN = "landscape_token";
  public static final String COL_TRACE_ID = "trace_id";
  public static final String COL_TIMESTAMP_SECONDS = "seconds";
  public static final String COL_TIMESTAMP_NANO = "nano_adjust";

  public static final String COL_START_TIME = "start_time";
  public static final String COL_END_TIME = "end_time";

  public static final String COL_SPAN_ID = "span_id";
  public static final String COL_SPAN_TRACE_ID = "span_trace_id";
  public static final String COL_SPAN_PARENT_ID = "span_parent_id";
  public static final String COL_SPAN_START_TIME = "span_start_time";
  public static final String COL_SPAN_END_TIME = "span_end_time";
  public static final String COL_SPAN_HASH = "span_hash";


  public static final String COL_SPANS = "spans";



  private final CqlSession dbSession;

  /**
   * @param session the CqlSession
   */
  @Inject
  public DBHelper(final CqlSession session) {
    this.dbSession = session;
  }

  public CqlSession getSession() {
    return this.dbSession;
  }

  /**
   * Initializes the database by creating necessary schemata. This is a no-op if the database is
   * already initalized;
   */
  public void initialize() {
    this.createKeySpace();
    this.createLandscapeRecordTable();
    this.registerCodecs();
  }

  /**
   * Creates a keyspace name "explorviz". No-op if this keyspace already exists.
   */
  private void createKeySpace() {
    final CreateKeyspace createKs = SchemaBuilder
        .createKeyspace(KEYSPACE_NAME)
        .ifNotExists()
        .withSimpleStrategy(1)
        .withDurableWrites(true);
    this.dbSession.execute(createKs.build());
  }

  public CodecRegistry getCodecRegistry() {
    return this.dbSession.getContext().getCodecRegistry();
  }

  /**
   * Creates the table "traces" which holds all {@link net.explorviz.avro.Trace} objects. No-op if this
   * table already exists.
   */
  private void createLandscapeRecordTable() {



    final CreateType createTimestampUdt = SchemaBuilder
        .createType(KEYSPACE_NAME, TYPE_TIMESTAMP)
        .ifNotExists()
        .withField(COL_TIMESTAMP_SECONDS, DataTypes.BIGINT)
        .withField(COL_TIMESTAMP_NANO, DataTypes.INT);


    final CreateType createSpanUdt = SchemaBuilder
        .createType(KEYSPACE_NAME, TYPE_SPAN)
        .ifNotExists()
        .withField(COL_SPAN_TRACE_ID, DataTypes.TEXT)
        .withField(COL_SPAN_ID, DataTypes.TEXT)
        .withField(COL_SPAN_PARENT_ID, DataTypes.TEXT)
        .withField(COL_SPAN_START_TIME, SchemaBuilder.udt(TYPE_TIMESTAMP, true))
        .withField(COL_SPAN_END_TIME, SchemaBuilder.udt(TYPE_TIMESTAMP, true))
        .withField(COL_SPAN_HASH, DataTypes.TEXT);


    final CreateTable createTraceTable = SchemaBuilder
        .createTable(KEYSPACE_NAME, TRACES_TABLE_NAME)
        .ifNotExists()
        .withPartitionKey(COL_TOKEN, DataTypes.TEXT)
        .withClusteringColumn(COL_TRACE_ID, DataTypes.TEXT)
        .withClusteringColumn(COL_START_TIME, SchemaBuilder.udt(TYPE_TIMESTAMP, true))
        .withClusteringColumn(COL_END_TIME, SchemaBuilder.udt(TYPE_TIMESTAMP, true))
        .withColumn(COL_SPANS, DataTypes.setOf(SchemaBuilder.udt(TYPE_SPAN, true), false));


    // Create index on start time for efficient range queries
    final CreateIndex createTSIndex = SchemaBuilder.createIndex("timestamp_index")
        .ifNotExists()
        .onTable(KEYSPACE_NAME, TRACES_TABLE_NAME)
        .andColumn(COL_START_TIME);


    this.dbSession.execute(createTimestampUdt.asCql());
    this.dbSession.execute(createSpanUdt.asCql());
    this.dbSession.execute(createTraceTable.asCql());
    this.dbSession.execute(createTSIndex.asCql());

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Created trace table and associated types");
    }

  }

  private void registerCodecs() {
    final CodecRegistry codecRegistry = this.getCodecRegistry();

    // Register Node coded
    final UserDefinedType timestampUdt =
        this.dbSession.getMetadata().getKeyspace(KEYSPACE_NAME)
            .flatMap(ks -> ks.getUserDefinedType(TYPE_TIMESTAMP))
            .orElseThrow(IllegalStateException::new);
    final TypeCodec<UdtValue> timestampUdtCodec = codecRegistry.codecFor(timestampUdt);
    final TimestampCodec timestampCodec = new TimestampCodec(timestampUdtCodec);
    ((MutableCodecRegistry) codecRegistry).register(timestampCodec);

    // Register Application codec
    final UserDefinedType spanUdt = this.dbSession.getMetadata().getKeyspace(KEYSPACE_NAME)
        .flatMap(ks -> ks.getUserDefinedType(TYPE_SPAN))
        .orElseThrow(IllegalStateException::new);
    final TypeCodec<UdtValue> appUdtCodec = codecRegistry.codecFor(spanUdt);
    final SpanCodec spanCodec = new SpanCodec(appUdtCodec, timestampCodec);
    ((MutableCodecRegistry) codecRegistry).register(spanCodec);

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Registered codecs");
    }
  }

}
