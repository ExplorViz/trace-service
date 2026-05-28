package net.explorviz.trace.kafka;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers a {@link StateListener} for the {@link KafkaStreams} object that is used as
 * ErrorStateListener. The Quarkus application does not shut down automatically if the Kafka Streams
 * thread goes into {@link State}.ERROR state. The Kafka streams ERROR state has no come back,
 * therefore the application is dead.
 */
@ApplicationScoped
public class ShutdownHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownHandler.class);

  @Inject
  /* default */ KafkaStreams streams;

  /* default */ void onStart(@Observes final StartupEvent ev) {
    this.streams.setStateListener(new ErrorStateListener());
  }

  /* default */ void onStop(@Observes final ShutdownEvent ev) {
    // nothing to do
  }

  private static class ErrorStateListener implements StateListener {

    @Override
    public void onChange(final State newState, final State oldState) {
      if (newState == State.ERROR) {

        LOGGER.atError().log("Kafka Streams thread died. Are Kafka topic initialized? "
            + "Quarkus application will shut down.");

        LOGGER.atError().log("About to system exit due to Kafka Streams Error.");

        Quarkus.asyncExit(-1);
        Quarkus.waitForExit();
        System.exit(-1); // NOPMD
      }

    }
  }
}
