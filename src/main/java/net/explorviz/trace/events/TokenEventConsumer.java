package net.explorviz.trace.events;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.avro.EventType;
import net.explorviz.avro.TokenEvent;

import net.explorviz.trace.service.TraceService;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ApplicationScoped
public class TokenEventConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TokenEventConsumer.class);

  private final TraceService service;

  @Inject
  public TokenEventConsumer(TraceService service) {
    this.service = service;
  }

  @Incoming("token-events")
  public void process(TokenEvent event) {
    LOGGER.info("Received event {}", event);
    if (event.getType() == EventType.DELETED) {
      service.deleteAll(event.getToken());
      LOGGER.info("Deleting traces for token {}", event.getToken());
    }
  }


}
