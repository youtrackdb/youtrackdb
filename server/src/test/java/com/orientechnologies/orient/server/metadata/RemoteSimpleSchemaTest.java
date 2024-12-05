package com.orientechnologies.orient.server.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.common.io.OFileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RemoteSimpleSchemaTest {

  private static final String SERVER_DIRECTORY = "./target/metadata-push";
  private OServer server;
  private YouTrackDB youTrackDB;
  private YTDatabaseSessionInternal database;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    youTrackDB = new YouTrackDB("remote:localhost", "root", "root",
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        RemoteSimpleSchemaTest.class.getSimpleName());
    database = (YTDatabaseSessionInternal) youTrackDB.open(
        RemoteSimpleSchemaTest.class.getSimpleName(), "admin", "admin");
  }

  @Test
  public void testCreateClassIfNotExist() {
    database.createClassIfNotExist("test");
    assertTrue(database.getMetadata().getSchema().existsClass("test"));
    assertTrue(database.getMetadata().getSchema().existsClass("TEST"));
    database.createClassIfNotExist("TEST");
    database.createClassIfNotExist("test");
  }

  @Test
  public void testNotCaseSensitiveDrop() {
    database.createClass("test");
    assertTrue(database.getMetadata().getSchema().existsClass("test"));
    database.getMetadata().getSchema().dropClass("TEST");
    assertFalse(database.getMetadata().getSchema().existsClass("test"));
  }

  @Test
  public void testWithSpecialCharacters() {
    database.createClass("test-foo");
    assertTrue(database.getMetadata().getSchema().existsClass("test-foo"));
    database.getMetadata().getSchema().dropClass("test-foo");
    assertFalse(database.getMetadata().getSchema().existsClass("test-foo"));
  }

  @After
  public void after() {
    database.close();
    youTrackDB.close();
    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBManager.instance().startup();
  }
}
