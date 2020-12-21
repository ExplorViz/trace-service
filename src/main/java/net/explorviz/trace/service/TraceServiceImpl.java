package net.explorviz.trace.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
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
    Optional<Collection<SpanDynamic>> spans = repository.getSpans(landscapeToken, traceId);
    if (spans.isPresent()) {
      try {
        Trace built = builder.build(spans.get());
        // Set token for trace here as the spans do not contain it
        built.setLandscapeToken(landscapeToken);
        result = Optional.of(built);
      } catch (IllegalArgumentException e) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Could not build a trace: {0}", e);
        }
      }
    }
    return result;
  }

  @Override
  public Collection<Trace> getBetween(final String landscapeToken, final Instant from,
                                      final Instant to) {
    List<Set<SpanDynamic>> spanSets = repository.getAllInRange(landscapeToken, from, to);

    List<Trace> traces = new ArrayList<>(spanSets.size());

    for (Set<SpanDynamic> spans: spanSets) {
      try {
        Trace t = builder.build(spans);
        t.setLandscapeToken(landscapeToken);
        traces.add(t);
      }catch (IllegalArgumentException e) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Could not build a trace: {0}", e);
        }
      }
    }
    return traces;
  }

  @Override
  public void deleteAll(final String landscapeToken) {
    repository.deleteAll(landscapeToken);
  }


}
