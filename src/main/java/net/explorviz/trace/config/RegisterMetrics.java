package net.explorviz.trace.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.quarkus.runtime.StartupEvent;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import net.explorviz.trace.kafka.SpanPersistingStream;

@ApplicationScoped
public class RegisterMetrics {

  private final MeterRegistry registry;
  private final SpanPersistingStream stream;


  @Inject
  public RegisterMetrics(final MeterRegistry registry, SpanPersistingStream stream) {
    this.registry = registry;
    this.stream = stream;
  }

  void onStart(@Observes final StartupEvent event) {
    PrometheusMeterRegistry prometheusMeterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    KafkaStreamsMetrics ksm = new KafkaStreamsMetrics(this.stream.getStream());
    ksm.bindTo(registry);
  }

}
