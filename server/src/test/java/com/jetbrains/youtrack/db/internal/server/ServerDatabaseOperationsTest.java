package com.jetbrains.youtrack.db.internal.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.server.config.ServerConfiguration;
import com.jetbrains.youtrack.db.internal.server.config.ServerUserConfiguration;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.logging.Level;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ServerDatabaseOperationsTest {

  private static final String SERVER_DIRECTORY = "./target/db";

  private YouTrackDBServer server;

  @Before
  public void before()
      throws ClassNotFoundException,
      MalformedObjectNameException,
      InstanceAlreadyExistsException,
      NotCompliantMBeanException,
      MBeanRegistrationException,
      NoSuchMethodException,
      IOException,
      InvocationTargetException,
      IllegalAccessException,
      InstantiationException {
    LogManager.instance().setConsoleLevel(Level.OFF.getName());
    ServerConfiguration conf = new ServerConfiguration();

    conf.handlers = new ArrayList<>();
    ServerUserConfiguration rootUser = new ServerUserConfiguration();
    rootUser.name = "root";
    rootUser.password = "root";
    rootUser.resources = "server.listDatabases";
    conf.users = new ServerUserConfiguration[]{rootUser};
    server = new YouTrackDBServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(conf);
    server.activate();

    server
        .getContext()
        .execute("create database " + ServerDatabaseOperationsTest.class.getSimpleName() +
            " memory users (admin identified by 'admin' role admin)");
    assertTrue(server.existsDatabase(ServerDatabaseOperationsTest.class.getSimpleName()));

    try (DatabaseSession session = server.openDatabase(
        ServerDatabaseOperationsTest.class.getSimpleName())) {
      EntityImpl securityConfig = new EntityImpl();
      securityConfig.fromJSON(
          IOUtils.readStreamAsString(
              this.getClass().getClassLoader().getResourceAsStream("security.json")),
          "noMap");
      server.getSecurity().reload((DatabaseSessionInternal) session, securityConfig);
    } finally {
      server.dropDatabase(ServerDatabaseOperationsTest.class.getSimpleName());
    }
  }

  @After
  public void after() {
    server.shutdown();

    YouTrackDBEnginesManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBEnginesManager.instance().startup();
  }

  @Test
  public void testServerLoginDatabase() {
    assertNotNull(server.authenticateUser("root", "root", "server.listDatabases"));
  }

  @Test
  public void testCreateOpenDatabase() {
    server
        .getContext()
        .execute("create database " + ServerDatabaseOperationsTest.class.getSimpleName()
            + " memory users (admin identified by 'admin' role admin)");
    assertTrue(server.existsDatabase(ServerDatabaseOperationsTest.class.getSimpleName()));
    DatabaseSession session = server.openDatabase(
        ServerDatabaseOperationsTest.class.getSimpleName());
    assertNotNull(session);
    session.close();
    server.dropDatabase(ServerDatabaseOperationsTest.class.getSimpleName());
  }
}
