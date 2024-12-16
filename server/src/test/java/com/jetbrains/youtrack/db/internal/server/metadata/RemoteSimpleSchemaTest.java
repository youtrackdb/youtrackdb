package com.jetbrains.youtrack.db.internal.server.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RemoteSimpleSchemaTest {

  private static final String SERVER_DIRECTORY = "./target/metadata-push";
  private YouTrackDBServer server;
  private YouTrackDB youTrackDB;
  private DatabaseSessionInternal database;

  @Before
  public void before() throws Exception {
    server = new YouTrackDBServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("youtrackdb-server-config.xml"));
    server.activate();

    youTrackDB = new YouTrackDBImpl("remote:localhost", "root", "root",
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        RemoteSimpleSchemaTest.class.getSimpleName());
    database = (DatabaseSessionInternal) youTrackDB.open(
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

    YouTrackDBEnginesManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBEnginesManager.instance().startup();
  }
}
