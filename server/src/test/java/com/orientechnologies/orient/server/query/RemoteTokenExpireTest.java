package com.orientechnologies.orient.server.query;

import static com.orientechnologies.orient.core.config.YTGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.enterprise.channel.binary.YTTokenSecurityException;
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
  private YTDatabaseSession session;
  private int oldPageSize;

  private final long expireTimeout = 500;

  @Before
  public void before() throws Exception {

    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
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
        YouTrackDBConfig.builder().addConfig(YTGlobalConfiguration.NETWORK_SOCKET_RETRY, 0).build();
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

    try (YTResultSet res = session.query("select from Some")) {

      Assert.assertEquals(0, res.stream().count());

    } catch (YTTokenSecurityException e) {

      Assert.fail("It should not get the exception");
    }
  }

  @Test
  public void itShouldNotFailWithCommand() {

    waitAndClean();

    session.activateOnCurrentThread();

    session.begin();
    try (YTResultSet res = session.command("insert into V set name = 'foo'")) {
      session.commit();

      Assert.assertEquals(1, res.stream().count());

    } catch (YTTokenSecurityException e) {

      Assert.fail("It should not get the exception");
    }
  }

  @Test
  public void itShouldNotFailWithScript() {

    waitAndClean();

    session.activateOnCurrentThread();

    try (YTResultSet res = session.execute("sql", "begin;insert into V set name = 'foo';commit;")) {

      Assert.assertEquals(1, res.stream().count());

    } catch (YTTokenSecurityException e) {

      Assert.fail("It should not get the exception");
    }
  }

  @Test
  public void itShouldFailWithQueryNext() throws InterruptedException {

    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(1);

    try (YTResultSet res = session.query("select from ORole")) {

      waitAndClean();
      session.activateOnCurrentThread();
      Assert.assertEquals(3, res.stream().count());

    } catch (YTTokenSecurityException e) {
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

    session.save(session.newElement("Some"));

    try (YTResultSet res = session.query("select from Some")) {
      Assert.assertEquals(1, res.stream().count());
    } catch (YTTokenSecurityException e) {
      Assert.fail("It should not get the expire exception");
    } finally {
      session.rollback();
    }
  }

  @Test
  public void itShouldFailAtBeingAndQuery() {

    session.begin();

    session.save(session.newElement("Some"));

    try (YTResultSet resultSet = session.query("select from Some")) {
      Assert.assertEquals(1, resultSet.stream().count());
    }
    waitAndClean();

    session.activateOnCurrentThread();

    try {
      session.query("select from Some");
    } catch (YTTokenSecurityException e) {
      session.rollback();
      return;
    }
    Assert.fail("It should not get the expire exception");
  }

  @Test
  public void itShouldNotFailWithRoundRobin() {

    ODatabasePool pool =
        new ODatabasePool(
            youTrackDB,
            RemoteTokenExpireTest.class.getSimpleName(),
            "admin",
            "admin",
            YouTrackDBConfig.builder()
                .addConfig(
                    YTGlobalConfiguration.CLIENT_CONNECTION_STRATEGY,
                    OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_CONNECT)
                .build());

    YTDatabaseSession session = pool.acquire();

    try (YTResultSet resultSet = session.query("select from Some")) {
      Assert.assertEquals(0, resultSet.stream().count());
    }

    waitAndClean();

    session.activateOnCurrentThread();

    try {
      try (YTResultSet resultSet = session.query("select from Some")) {
        Assert.assertEquals(0, resultSet.stream().count());
      }
    } catch (YTTokenSecurityException e) {
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
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBManager.instance().startup();
  }
}
