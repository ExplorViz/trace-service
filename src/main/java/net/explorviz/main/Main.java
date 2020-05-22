package net.explorviz.main;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import net.explorviz.kafka.SpanToTraceReconstructorStream;
import org.jboss.logging.Logger;

@ApplicationScoped
public class Main {

  private static final Logger LOGGER = Logger.getLogger(Main.class);

  @Inject
  SpanToTraceReconstructorStream traceReconstructor;

  void onStart(@Observes StartupEvent ev) {
    LOGGER.info("The application is starting...");

    traceReconstructor.startStreamProcessing();
  }

  void onStop(@Observes ShutdownEvent ev) {
    LOGGER.info("The application is stopping...");
  }

}

