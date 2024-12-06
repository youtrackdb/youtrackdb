package com.orientechnologies.orient.client.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.LiveQueryResultListener;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
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
public class ORemoteLiveQueryPushTest {

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
  private ORemoteConnectionManager connectionManager;

  @Mock
  private DatabaseSessionInternal database;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    storage =
        new StorageRemote(
            new ORemoteURLs(new String[]{}, new ContextConfiguration()),
            "none",
            null,
            "",
            connectionManager,
            null);
  }

  @Test
  public void testLiveEvents() {
    MockLiveListener mock = new MockLiveListener();
    storage.registerLiveListener(10, new OLiveQueryClientListener(database, mock));
    List<OLiveQueryResult> events = new ArrayList<>();
    events.add(
        new OLiveQueryResult(OLiveQueryResult.CREATE_EVENT, new ResultInternal(database), null));
    events.add(
        new OLiveQueryResult(
            OLiveQueryResult.UPDATE_EVENT, new ResultInternal(database),
            new ResultInternal(database)));
    events.add(
        new OLiveQueryResult(OLiveQueryResult.DELETE_EVENT, new ResultInternal(database), null));

    OLiveQueryPushRequest request =
        new OLiveQueryPushRequest(10, OLiveQueryPushRequest.END, events);
    request.execute(null, storage);
    assertEquals(1, mock.countCreate);
    assertEquals(1, mock.countUpdate);
    assertEquals(1, mock.countDelete);
  }
}
