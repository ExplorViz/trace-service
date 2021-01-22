package net.explorviz.trace.injection;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.quarkus.arc.DefaultBean;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;


/**
 * Factory for the client of the schema registry.
 */
@Dependent
public class SchemaRegistryClientProducer {

  private static final int IDENTITY_MAP_CAPACITY = 10;
  
  @ConfigProperty(name = "explorviz.schema-registry.url")
  String schemaRegistryUrl; //NOCS

  @Produces
  @DefaultBean
  public SchemaRegistryClient schemaRegistryClient() {
    return new CachedSchemaRegistryClient("http://" + this.schemaRegistryUrl, IDENTITY_MAP_CAPACITY);
  }
}
