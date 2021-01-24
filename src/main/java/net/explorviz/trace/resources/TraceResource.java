package net.explorviz.trace.resources;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import net.explorviz.avro.Trace;
import net.explorviz.trace.service.TraceService;

/**
 * HTTP resource for accessing traces.
 */
@Path("/v2/landscapes")
public class TraceResource {



  private static final long MIN_SECONDS = 1_577_836_800; // 01.01.2020 00:00

  private final TraceService traceService;

  @Inject
  public TraceResource(final TraceService traceService) {
    this.traceService = traceService;
  }

  @GET
  @Path("/{token}/dynamic/{traceid}")
  @Produces(MediaType.APPLICATION_JSON)
  public Trace getTrace(@PathParam("token") final String landscapeToken,
      @PathParam("traceid") final String traceId) {

    final Optional<Trace> trace = this.traceService.getById(landscapeToken, traceId);
    if (trace.isPresent()) {
      return trace.get();
    } else {
      throw new NotFoundException();
    }
  }

  @GET
  @Path("/{token}/dynamic")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<Trace> getTraces(@PathParam("token") final String landscapeToken,
      @QueryParam("from") final Long fromMs,
      @QueryParam("to") final Long toMs) {

    Instant from = Instant.ofEpochSecond(MIN_SECONDS, 0);
    Instant to = Instant.now();
    final int c = (fromMs == null ? 0 : 1) + (toMs == null ? 0 : 2);
    switch (c) {
      case 1: // from is given
        from = Instant.ofEpochMilli(fromMs);
        break;
      case 2: // to is given
        to = Instant.ofEpochMilli(toMs);
        break;
      case 3: // both given // NOCS
        from = Instant.ofEpochMilli(fromMs);
        to = Instant.ofEpochMilli(toMs);
        break;
      default:
        break;
    }

    return this.traceService.getBetween(landscapeToken, from, to);
  }

}
