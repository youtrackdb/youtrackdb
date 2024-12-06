package com.orientechnologies.orient.server;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public class BaseServerMemoryDatabase {

  protected DatabaseSessionInternal db;
  protected YouTrackDB context;
  @Rule
  public TestName name = new TestName();
  protected OServer server;

  @Before
  public void beforeTest() {
    server = new OServer(false);
    try {
      server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
      server.activate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    context = new YouTrackDB("remote:localhost", "root", "root", YouTrackDBConfig.defaultConfig());
    context
        .execute(
            "create database "
                + name.getMethodName()
                + " memory users(admin identified by 'adminpwd' role admin) ")
        .close();
    db = (DatabaseSessionInternal) context.open(name.getMethodName(), "admin", "adminpwd");
  }

  @After
  public void afterTest() {
    db.close();
    context.drop(name.getMethodName());
    context.close();
    String directory = server.getDatabaseDirectory();
    server.shutdown();
    FileUtils.deleteRecursively(new File(directory));
  }
}
