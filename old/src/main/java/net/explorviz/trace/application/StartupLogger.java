package net.explorviz.trace.application;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class StartupLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartupLogger.class);

  @ConfigProperty(name = "explorviz.span.api.timeverification.enabled")
  /* default */ boolean isTimeVerificationEnabled;

  void onStart(@Observes final StartupEvent ev) {
    LOGGER.atInfo().log("The application is starting...");

    LOGGER.atInfo().addArgument(isTimeVerificationEnabled).log("API time ranges are enabled: {}");
  }

}
