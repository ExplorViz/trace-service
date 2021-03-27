package net.explorviz.trace.events;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.EventType;
import net.explorviz.avro.TokenEvent;
import net.explorviz.trace.persistence.TraceReactiveService;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Waits for and reacts to token-events dispatched by the User-Service. Such events are read from a
 * corresponding Kafka topic and contain information about changes to tokens (e.g. deletions).
 */

@ApplicationScoped
public class TokenEventConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TokenEventConsumer.class);

  private final TraceReactiveService service;

  @Inject
  public TokenEventConsumer(TraceReactiveService traceReactiveService) {
    this.service = traceReactiveService;
  }


  /**
   * Processes token-events in a background, called by reactive messaging framework. If a token was
   * deleted, all corresponding entries are removed from the database.
   *
   * @param event the token-event
   */

  @Incoming("token-events")
  public void process(final TokenEvent event) {
    LOGGER.info("Received event {}", event);
    if (event.getType() == EventType.DELETED) {
      // this.service.deleteAll(event.getToken());
      LOGGER.info("Deleting traces for token {}", event.getToken());
    } else if (event.getType() == EventType.CLONED) {
      this.service.cloneAllAsync(event.getToken(), event.getClonedToken()).subscribe().with(
        item -> LOGGER.info("Duplicated " + item.getLandscapeToken()),
        failure -> LOGGER.error("Failed to duplicate", failure),
        () -> LOGGER.info("Duplication complete"));
    }
  }


}
