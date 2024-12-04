package com.orientechnologies.orient.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
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

public class OServerDatabaseOperationsTest {

  private static final String SERVER_DIRECTORY = "./target/db";

  private OServer server;

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
    OLogManager.instance().setConsoleLevel(Level.OFF.getName());
    OServerConfiguration conf = new OServerConfiguration();

    conf.handlers = new ArrayList<>();
    OServerUserConfiguration rootUser = new OServerUserConfiguration();
    rootUser.name = "root";
    rootUser.password = "root";
    rootUser.resources = "server.listDatabases";
    conf.users = new OServerUserConfiguration[]{rootUser};
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(conf);
    server.activate();

    server
        .getContext()
        .execute("create database " + OServerDatabaseOperationsTest.class.getSimpleName() +
            " memory users (admin identified by 'admin' role admin)");
    assertTrue(server.existsDatabase(OServerDatabaseOperationsTest.class.getSimpleName()));

    try (YTDatabaseSession session = server.openDatabase(
        OServerDatabaseOperationsTest.class.getSimpleName())) {
      YTDocument securityConfig = new YTDocument();
      securityConfig.fromJSON(
          OIOUtils.readStreamAsString(
              this.getClass().getClassLoader().getResourceAsStream("security.json")),
          "noMap");
      server.getSecurity().reload((YTDatabaseSessionInternal) session, securityConfig);
    } finally {
      server.dropDatabase(OServerDatabaseOperationsTest.class.getSimpleName());
    }
  }

  @After
  public void after() {
    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBManager.instance().startup();
  }

  @Test
  public void testServerLoginDatabase() {
    assertNotNull(server.authenticateUser("root", "root", "server.listDatabases"));
  }

  @Test
  public void testCreateOpenDatabase() {
    server
        .getContext()
        .execute("create database " + OServerDatabaseOperationsTest.class.getSimpleName()
            + " memory users (admin identified by 'admin' role admin)");
    assertTrue(server.existsDatabase(OServerDatabaseOperationsTest.class.getSimpleName()));
    YTDatabaseSession session = server.openDatabase(
        OServerDatabaseOperationsTest.class.getSimpleName());
    assertNotNull(session);
    session.close();
    server.dropDatabase(OServerDatabaseOperationsTest.class.getSimpleName());
  }
}
