package com.jetbrains.youtrack.db.internal.server.query;

import static com.jetbrains.youtrack.db.api.config.GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.session.SessionPool;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemote;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.SessionPoolImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.TokenSecurityException;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import com.jetbrains.youtrack.db.internal.server.token.TokenHandlerImpl;
import java.io.File;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class RemoteTokenExpireTest {

  private static final String SERVER_DIRECTORY = "./target/token";
  private YouTrackDBServer server;
  private YouTrackDBImpl youTrackDB;
  private DatabaseSession session;
  private int oldPageSize;

  private final long expireTimeout = 500;

  @Before
  public void before() throws Exception {

    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    server = new YouTrackDBServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("youtrackdb-server-config.xml"));

    server.activate();

    var token = (TokenHandlerImpl) server.getTokenHandler();
    token.setSessionInMills(expireTimeout);

    youTrackDB = new YouTrackDBImpl("remote:localhost", "root", "root",
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        RemoteTokenExpireTest.class.getSimpleName());
    session = youTrackDB.open(RemoteTokenExpireTest.class.getSimpleName(), "admin", "admin");
    session.createClass("Some");
    oldPageSize = QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(10);

    session.close();
    youTrackDB.close();

    var config =
        YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(GlobalConfiguration.NETWORK_SOCKET_RETRY, 0).build();
    youTrackDB = new YouTrackDBImpl("remote:localhost", "root", "root", config);
    session = youTrackDB.open(RemoteTokenExpireTest.class.getSimpleName(), "admin", "admin");
  }

  private void clean() {
    server.getClientConnectionManager().cleanExpiredConnections();
  }

  private void waitAndClean(long ms) {
    try {
      Thread.sleep(ms);
      clean();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void waitAndClean() {
    waitAndClean(expireTimeout);
  }

  @Test
  public void itShouldNotFailWithQuery() {

    waitAndClean();

    session.activateOnCurrentThread();

    try (var res = session.query("select from Some")) {

      Assert.assertEquals(0, res.stream().count());

    } catch (TokenSecurityException e) {

      Assert.fail("It should not get the exception");
    }
  }

  @Test
  public void itShouldNotFailWithCommand() {

    waitAndClean();

    session.activateOnCurrentThread();

    session.begin();
    try (var res = session.command("insert into V set name = 'foo'")) {
      session.commit();

      Assert.assertEquals(1, res.stream().count());

    } catch (TokenSecurityException e) {

      Assert.fail("It should not get the exception");
    }
  }

  @Test
  public void itShouldNotFailWithScript() {

    waitAndClean();

    session.activateOnCurrentThread();

    try (var res = session.execute("sql", "begin;insert into V set name = 'foo';commit;")) {

      Assert.assertEquals(1, res.stream().count());

    } catch (TokenSecurityException e) {

      Assert.fail("It should not get the exception");
    }
  }

  @Test
  public void itShouldFailWithQueryNext() throws InterruptedException {

    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(1);

    try (var res = session.query("select from ORole")) {

      waitAndClean();
      session.activateOnCurrentThread();
      Assert.assertEquals(3, res.stream().count());

    } catch (TokenSecurityException e) {
      return;
    } finally {
      QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(10);
    }
    Assert.fail("It should get an exception");
  }

  @Test
  public void itShouldNotFailWithNewTXAndQuery() {

    waitAndClean();

    session.activateOnCurrentThread();

    session.begin();

    session.newEntity("Some");

    try (var res = session.query("select from Some")) {
      Assert.assertEquals(1, res.stream().count());
    } catch (TokenSecurityException e) {
      Assert.fail("It should not get the expire exception");
    } finally {
      session.rollback();
    }
  }

  @Test
  public void itShouldFailAtBeingAndQuery() {

    session.begin();

    session.newEntity("Some");

    try (var resultSet = session.query("select from Some")) {
      Assert.assertEquals(1, resultSet.stream().count());
    }
    waitAndClean();

    session.activateOnCurrentThread();

    try {
      session.query("select from Some");
    } catch (TokenSecurityException e) {
      session.rollback();
      return;
    }
    Assert.fail("It should not get the expire exception");
  }

  @Test
  public void itShouldNotFailWithRoundRobin() {

    SessionPool pool =
        new SessionPoolImpl(
            youTrackDB,
            RemoteTokenExpireTest.class.getSimpleName(),
            "admin",
            "admin",
            (YouTrackDBConfigImpl) YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(
                    GlobalConfiguration.CLIENT_CONNECTION_STRATEGY,
                    StorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_CONNECT)
                .build());

    var session = pool.acquire();

    try (var resultSet = session.query("select from Some")) {
      Assert.assertEquals(0, resultSet.stream().count());
    }

    waitAndClean();

    session.activateOnCurrentThread();

    try {
      try (var resultSet = session.query("select from Some")) {
        Assert.assertEquals(0, resultSet.stream().count());
      }
    } catch (TokenSecurityException e) {
      Assert.fail("It should  get the expire exception");
    }
    pool.close();
  }

  @After
  public void after() {
    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(oldPageSize);
    session.activateOnCurrentThread();
    session.close();
    youTrackDB.close();
    server.shutdown();

    YouTrackDBEnginesManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBEnginesManager.instance().startup();
  }
}
