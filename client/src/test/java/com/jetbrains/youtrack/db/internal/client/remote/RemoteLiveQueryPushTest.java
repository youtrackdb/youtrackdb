package com.jetbrains.youtrack.db.internal.client.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.query.LiveQueryResultListener;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.client.remote.message.LiveQueryPushRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.live.LiveQueryResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 *
 */
public class RemoteLiveQueryPushTest {

  private static class MockLiveListener implements LiveQueryResultListener {

    public int countCreate = 0;
    public int countUpdate = 0;
    public int countDelete = 0;
    public boolean end;

    @Override
    public void onCreate(DatabaseSession database, Result data) {
      countCreate++;
    }

    @Override
    public void onUpdate(DatabaseSession database, Result before, Result after) {
      countUpdate++;
    }

    @Override
    public void onDelete(DatabaseSession database, Result data) {
      countDelete++;
    }

    @Override
    public void onError(DatabaseSession database, BaseException exception) {
    }

    @Override
    public void onEnd(DatabaseSession database) {
      assertFalse(end);
      end = true;
    }
  }

  private StorageRemote storage;

  @Mock
  private RemoteConnectionManager connectionManager;

  @Mock
  private DatabaseSessionInternal database;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    storage =
        new StorageRemote(
            new RemoteURLs(new String[]{}, new ContextConfiguration()),
            "none",
            null,
            "",
            connectionManager,
            null);
  }

  @Test
  public void testLiveEvents() {
    MockLiveListener mock = new MockLiveListener();
    storage.registerLiveListener(10, new LiveQueryClientListener(database, mock));
    List<LiveQueryResult> events = new ArrayList<>();
    events.add(
        new LiveQueryResult(LiveQueryResult.CREATE_EVENT, new ResultInternal(database), null));
    events.add(
        new LiveQueryResult(
            LiveQueryResult.UPDATE_EVENT, new ResultInternal(database),
            new ResultInternal(database)));
    events.add(
        new LiveQueryResult(LiveQueryResult.DELETE_EVENT, new ResultInternal(database), null));

    LiveQueryPushRequest request =
        new LiveQueryPushRequest(10, LiveQueryPushRequest.END, events);
    request.execute(null, storage);
    assertEquals(1, mock.countCreate);
    assertEquals(1, mock.countUpdate);
    assertEquals(1, mock.countDelete);
  }
}
