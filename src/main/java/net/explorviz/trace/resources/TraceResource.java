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
import net.explorviz.trace.persistence.TraceReactiveService;
import net.explorviz.trace.persistence.dao.Trace;

/**
 * HTTP resource for accessing traces.
 */
@Path("v2/landscapes")
public class TraceResource {

  private static final long MIN_SECONDS = 1_577_836_800; // 01.01.2020 00:00

  private final TraceReactiveService service;

  @Inject
  public TraceResource(final TraceReactiveService service) {
    this.service = service;
  }

  @GET
  @Path("{token}/dynamic/{traceid}")
  @Produces(MediaType.APPLICATION_JSON)
  public Multi<Trace> getTrace(@PathParam("token") final String landscapeToken,
      @PathParam("traceid") final String traceId) {

    return this.service.get(landscapeToken).filter(t -> t.getTraceId().equals(traceId));

    // return Multi.createFrom().empty();

    // return Multi.createFrom()
    // .item(new Trace(landscapeToken, traceId, spanList.get(0).getStartTime(),
    // spanList.get(spanList.size() - 1).getEndTime(), 5, 5,
    // 5, spanList));
  }

  @GET
  @Path("{token}/dynamic")
  @Produces(MediaType.APPLICATION_JSON)
  public Multi<Trace> getTraces(@PathParam("token") final String landscapeToken,
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

    return this.service.get(landscapeToken);

    // return this.service.get(landscapeToken).filter(t -> t.getStartTime().getSeconds() >= fromMs)
    // .filter(t -> t.getEndTime().getSeconds() <= toMs);
  }


}
