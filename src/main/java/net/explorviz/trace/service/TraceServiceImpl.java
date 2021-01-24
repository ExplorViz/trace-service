package net.explorviz.trace.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.explorviz.avro.SpanDynamic;
import net.explorviz.avro.Trace;
import net.explorviz.trace.persistence.SpanRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides method for accessing traces.
 */
@Singleton
public class TraceServiceImpl implements TraceService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceBuilderImpl.class);

  private final SpanRepository repository;
  private final TraceBuilder builder;

  @Inject
  public TraceServiceImpl(final SpanRepository repository, final TraceBuilder builder) {
    this.repository = repository;
    this.builder = builder;
  }

  @Override
  public Optional<Trace> getById(final String landscapeToken, final String traceId) {
    Optional<Trace> result = Optional.empty();
    final Optional<Collection<SpanDynamic>> spans =
        this.repository.getSpans(landscapeToken, traceId);
    if (spans.isPresent()) {
      try {
        final Trace built = this.builder.build(spans.get());
        // Set token for trace here as the spans do not contain it
        built.setLandscapeToken(landscapeToken);
        result = Optional.of(built);
      } catch (final IllegalArgumentException e) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Could not build a trace: {0}", e); // NOCS
        }
      }
    }
    return result;
  }

  @Override
  public Collection<Trace> getBetween(final String landscapeToken, final Instant from,
      final Instant to) {
    final List<Set<SpanDynamic>> spanSets = this.repository.getAllInRange(landscapeToken, from, to);

    final List<Trace> traces = new ArrayList<>(spanSets.size());

    for (final Set<SpanDynamic> spans : spanSets) {
      try {
        final Trace t = this.builder.build(spans);
        t.setLandscapeToken(landscapeToken);
        traces.add(t);
      } catch (final IllegalArgumentException e) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Could not build a trace: {0}", e);
        }
      }
    }
    return traces;
  }

  @Override
  public void deleteAll(final String landscapeToken) {
    this.repository.deleteAll(landscapeToken);
  }


}
