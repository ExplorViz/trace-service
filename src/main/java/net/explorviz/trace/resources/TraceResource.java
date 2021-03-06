package net.explorviz.trace.resources;

import io.smallrye.mutiny.Multi;
import java.time.Instant;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import net.explorviz.trace.persistence.dao.Trace;
import net.explorviz.trace.service.TraceRepository;

/**
 * HTTP resource for accessing traces.
 */
@Path("v2/landscapes")
public class TraceResource {

  private static final long MIN_SECONDS = 1_577_836_800; // 01.01.2020 00:00

  private final TraceRepository repository;

  @Inject
  public TraceResource(final TraceRepository repository) {
    this.repository = repository;
  }

  @GET
  @Path("{token}/dynamic/{traceid}")
  @Produces(MediaType.APPLICATION_JSON)
  public Multi<Trace> getTrace(@PathParam("token") final String landscapeToken,
      @PathParam("traceid") final String traceId) {
    return this.repository.getByTraceId(landscapeToken, traceId);
  }

  @GET
  @Path("{token}/dynamic")
  @Produces(MediaType.APPLICATION_JSON)
  public Multi<Trace> getTraces(@PathParam("token") final String landscapeToken,
      @QueryParam("from") final Long fromMs,
      @QueryParam("to") final Long toMs) {

    long from = MIN_SECONDS;
    long to = Instant.now().toEpochMilli();

    final int c = (fromMs == null ? 0 : 1) + (toMs == null ? 0 : 2);
    switch (c) {
      case 1: // from is given
        from = fromMs;
        break;
      case 2: // to is given
        to = toMs;
        break;
      case 3: // both given // NOCS
        from = fromMs;
        to = toMs;
        break;
      default:
        break;
    }

    return this.repository.getByStartTimeAndEndTime(landscapeToken, from, to);
  }


}
