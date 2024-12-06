package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.sql.query.LiveQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.LiveResultListener;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class OLiveQueryShotdownTest {

  private OServer server;

  public void bootServer() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    OServerAdmin server = new OServerAdmin("remote:localhost");
    server.connect("root", "root");
    server.createDatabase(OLiveQueryShotdownTest.class.getSimpleName(), "graph", "memory");
  }

  public void shutdownServer() {
    server.shutdown();
    YouTrackDBManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    YouTrackDBManager.instance().startup();
  }

  @Test
  public void testShutDown() throws Exception {
    bootServer();
    DatabaseSessionInternal db =
        new DatabaseDocumentTx(
            "remote:localhost/" + OLiveQueryShotdownTest.class.getSimpleName());
    db.open("admin", "admin");
    db.getMetadata().getSchema().createClass("Test");
    final CountDownLatch error = new CountDownLatch(1);
    db.command(
            new LiveQuery(
                "live select from Test",
                new LiveResultListener() {

                  @Override
                  public void onUnsubscribe(int iLiveToken) {
                  }

                  @Override
                  public void onLiveResult(int iLiveToken, RecordOperation iOp)
                      throws BaseException {
                  }

                  @Override
                  public void onError(int iLiveToken) {
                    error.countDown();
                  }
                }))
        .execute(db);

    shutdownServer();

    assertTrue("onError method never called on shutdow", error.await(2, TimeUnit.SECONDS));
  }
}
