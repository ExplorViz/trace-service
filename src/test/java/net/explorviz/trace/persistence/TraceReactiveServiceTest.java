package net.explorviz.trace.persistence;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import net.explorviz.trace.persistence.dao.Trace;
import net.explorviz.trace.service.TraceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TraceReactiveServiceTest {

  private TraceReactiveService mockTraceReactiveService;

  private TraceRepository testee;

  private static final String ORIGINAL_TOKEN = "12345";
  private static final String NEW_TOKEN = "abcde";

  @BeforeEach
  public void setup() {
    mockTraceReactiveService = Mockito.mock(TraceReactiveService.class);
    var traces = Multi.createFrom().items(new Trace(), new Trace());
    Mockito.when(mockTraceReactiveService.getAllAsync(ORIGINAL_TOKEN)).thenReturn(traces);
    testee = new TraceRepository(mockTraceReactiveService);
  }

  @Test
  public void testClone() {
    testee.cloneAllAsync(NEW_TOKEN, ORIGINAL_TOKEN).map(Trace::getLandscapeToken).subscribe()
        .withSubscriber(AssertSubscriber.create(10)).assertCompleted().assertItems(NEW_TOKEN, NEW_TOKEN);
  }
}