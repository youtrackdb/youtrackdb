package com.orientechnologies.orient.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.orientechnologies.orient.client.remote.ORemoteConnectionManager;
import com.orientechnologies.orient.client.remote.RemoteConnectionPool;
import com.orientechnologies.orient.client.remote.YouTrackDBRemote;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SocketIdleCleanupIT {

  private OServer server;

  @Before
  public void before()
      throws IOException,
      InstantiationException,
      InvocationTargetException,
      NoSuchMethodException,
      MBeanRegistrationException,
      IllegalAccessException,
      InstanceAlreadyExistsException,
      NotCompliantMBeanException,
      ClassNotFoundException,
      MalformedObjectNameException {
    String classpath = System.getProperty("java.class.path");
    System.out.println("Class path " + classpath);
    server =
        OServer.startFromStreamConfig(
            this.getClass().getResourceAsStream("orientdb-server-config.xml"));
  }

  @Test
  public void test() throws InterruptedException {
    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.CLIENT_CHANNEL_IDLE_CLOSE, true)
            .addConfig(GlobalConfiguration.CLIENT_CHANNEL_IDLE_TIMEOUT, 1)
            .build();
    YouTrackDB orientdb = new YouTrackDB("remote:localhost", "root", "root", config);
    orientdb.execute("create database test memory users (admin identified by 'admin' role admin)");
    DatabaseSession session = orientdb.open("test", "admin", "admin");
    session.save(session.newVertex("V"));
    Thread.sleep(2000);
    YouTrackDBRemote remote = (YouTrackDBRemote) YouTrackDBInternal.extract(orientdb);
    ORemoteConnectionManager connectionManager = remote.getConnectionManager();
    RemoteConnectionPool pool =
        connectionManager.getPool(connectionManager.getURLs().iterator().next());
    assertFalse(pool.getPool().getResources().iterator().next().isConnected());
    try (ResultSet result = session.query("select from V")) {
      assertEquals(result.stream().count(), 1);
    }
  }

  @After
  public void after() {
    server.shutdown();
  }
}
