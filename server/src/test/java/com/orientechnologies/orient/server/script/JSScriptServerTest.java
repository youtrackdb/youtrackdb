package com.orientechnologies.orient.server.script;

import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class JSScriptServerTest {

  @Rule
  public TestName name = new TestName();

  private OServer server;

  @Before
  public void before() throws Exception {

    server =
        OServer.startFromStreamConfig(
            getClass().getResourceAsStream("orientdb-server-javascript-config.xml"));
  }

  @Test
  public void jsPackagesFromConfigTest() {

    YouTrackDB youTrackDB =
        new YouTrackDB("remote:localhost", "root", "root", YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        name.getMethodName());
    try (var db = youTrackDB.open(name.getMethodName(), "admin", "admin")) {
      try (ResultSet resultSet = db.execute("javascript", "new java.math.BigDecimal(1.0);")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }

      try (ResultSet resultSet = db.execute("javascript", "new java.util.ArrayList();")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }

    } finally {

      youTrackDB.drop(name.getMethodName());
      youTrackDB.close();
    }
  }

  @After
  public void after() {
    server.shutdown();
  }
}
