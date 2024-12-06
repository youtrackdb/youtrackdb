package com.orientechnologies.orient.server.query;

import static com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabasePool;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.TokenSecurityException;
import com.orientechnologies.orient.client.remote.StorageRemote;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.token.OTokenHandlerImpl;
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
  private OServer server;
  private YouTrackDB youTrackDB;
  private DatabaseSession session;
  private int oldPageSize;

  private final long expireTimeout = 500;

  @Before
  public void before() throws Exception {

    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));

    server.activate();

    OTokenHandlerImpl token = (OTokenHandlerImpl) server.getTokenHandler();
    token.setSessionInMills(expireTimeout);

    youTrackDB = new YouTrackDB("remote:localhost", "root", "root",
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
        YouTrackDBConfig.builder().addConfig(GlobalConfiguration.NETWORK_SOCKET_RETRY, 0).build();
    youTrackDB = new YouTrackDB("remote:localhost", "root", "root", config);
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

    try (ResultSet res = session.query("select from Some")) {

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
    try (ResultSet res = session.command("insert into V set name = 'foo'")) {
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

    try (ResultSet res = session.execute("sql", "begin;insert into V set name = 'foo';commit;")) {

      Assert.assertEquals(1, res.stream().count());

    } catch (TokenSecurityException e) {

      Assert.fail("It should not get the exception");
    }
  }

  @Test
  public void itShouldFailWithQueryNext() throws InterruptedException {

    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(1);

    try (ResultSet res = session.query("select from ORole")) {

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

    session.save(session.newEntity("Some"));

    try (ResultSet res = session.query("select from Some")) {
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

    session.save(session.newEntity("Some"));

    try (ResultSet resultSet = session.query("select from Some")) {
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

    DatabasePool pool =
        new DatabasePool(
            youTrackDB,
            RemoteTokenExpireTest.class.getSimpleName(),
            "admin",
            "admin",
            YouTrackDBConfig.builder()
                .addConfig(
                    GlobalConfiguration.CLIENT_CONNECTION_STRATEGY,
                    StorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_CONNECT)
                .build());

    DatabaseSession session = pool.acquire();

    try (ResultSet resultSet = session.query("select from Some")) {
      Assert.assertEquals(0, resultSet.stream().count());
    }

    waitAndClean();

    session.activateOnCurrentThread();

    try {
      try (ResultSet resultSet = session.query("select from Some")) {
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

    YouTrackDBManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBManager.instance().startup();
  }
}
