package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.YourTracks;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.query.LiveQueryResultListener;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.client.remote.ServerAdmin;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class LiveQueryShutdownTest {

  private OServer server;

  public void bootServer() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    ServerAdmin server = new ServerAdmin("remote:localhost");
    server.connect("root", "root");
    server.createDatabase(LiveQueryShutdownTest.class.getSimpleName(), "graph", "memory");
  }

  public void shutdownServer() {
    server.shutdown();
    YouTrackDBEnginesManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    YouTrackDBEnginesManager.instance().startup();
  }

  @Test
  public void testShutDown() throws Exception {
    bootServer();
    final CountDownLatch end = new CountDownLatch(1);
    try (var youTrackDbManager = YourTracks.remote("remote:localhost", "root", "root")) {
      youTrackDbManager.createIfNotExists(LiveQueryShutdownTest.class.getSimpleName(),
          DatabaseType.MEMORY, "admin", "admin", "admin");
      try (var db = youTrackDbManager.open(
          LiveQueryShutdownTest.class.getSimpleName(), "admin", "admin")) {
        db.getSchema().createClass("Test");
        db.live(
            "live select from Test",
            new LiveQueryResultListener() {

              @Override
              public void onCreate(DatabaseSession database, Result data) {

              }

              @Override
              public void onUpdate(DatabaseSession database, Result before, Result after) {

              }

              @Override
              public void onDelete(DatabaseSession database, Result data) {

              }

              @Override
              public void onError(DatabaseSession database, BaseException exception) {

              }

              @Override
              public void onEnd(DatabaseSession database) {
                end.countDown();
              }
            });
      }
    }
    shutdownServer();

    assertTrue("onEnd method never called on shutdown", end.await(2, TimeUnit.SECONDS));
  }
}
