package com.orientechnologies.orient.server.metadata;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Oxygen;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.Locale;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class MetadataPushTest {

  private static final String SERVER_DIRECTORY = "./target/metadata-push";
  private OServer server;
  private OxygenDB oxygenDB;
  private ODatabaseSession database;

  private OxygenDB secondOxygenDB;
  private ODatabaseSessionInternal secondDatabase;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    oxygenDB = new OxygenDB("remote:localhost", "root", "root", OxygenDBConfig.defaultConfig());
    oxygenDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        MetadataPushTest.class.getSimpleName());
    database = oxygenDB.open(MetadataPushTest.class.getSimpleName(), "admin", "admin");

    secondOxygenDB = new OxygenDB("remote:localhost", OxygenDBConfig.defaultConfig());
    secondDatabase =
        (ODatabaseSessionInternal)
            oxygenDB.open(MetadataPushTest.class.getSimpleName(), "admin", "admin");
  }

  @After
  public void after() {
    database.activateOnCurrentThread();
    database.close();
    oxygenDB.close();
    secondDatabase.activateOnCurrentThread();
    secondDatabase.close();
    secondOxygenDB.close();
    server.shutdown();

    Oxygen.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Oxygen.instance().startup();
  }

  @Test
  public void testStorageUpdate() throws Exception {
    database.activateOnCurrentThread();
    database.command(" ALTER DATABASE LOCALELANGUAGE  ?", Locale.GERMANY.getLanguage());
    // Push done in background for now, do not guarantee update before command return.
    secondDatabase.activateOnCurrentThread();
    BaseMemoryDatabase.assertWithTimeout(
        secondDatabase,
        () -> {
          secondDatabase.activateOnCurrentThread();
          assertEquals(
              secondDatabase.get(ODatabaseSession.ATTRIBUTES.LOCALELANGUAGE),
              Locale.GERMANY.getLanguage());
        });
  }

  @Test
  public void testSchemaUpdate() throws Exception {
    database.activateOnCurrentThread();
    database.command(" create class X");

    // Push done in background for now, do not guarantee update before command return.
    secondDatabase.activateOnCurrentThread();
    BaseMemoryDatabase.assertWithTimeout(
        secondDatabase,
        () -> {
          secondDatabase.activateOnCurrentThread();
          assertTrue(secondDatabase.getMetadata().getSchema().existsClass("X"));
        });
  }

  @Test
  public void testIndexManagerUpdate() throws Exception {
    database.activateOnCurrentThread();
    database.command(" create class X");
    database.command(" create property X.y STRING");
    database.command(" create index X.y on X(y) NOTUNIQUE");
    // Push done in background for now, do not guarantee update before command return.
    secondDatabase.activateOnCurrentThread();
    BaseMemoryDatabase.assertWithTimeout(
        secondDatabase,
        () ->
            assertTrue(secondDatabase.getMetadata().getIndexManagerInternal().existsIndex("X.y")));
  }

  @Test
  public void testFunctionUpdate() throws Exception {
    database.activateOnCurrentThread();
    database.begin();
    database.command("CREATE FUNCTION test \"print('\\nTest!')\"");
    database.commit();

    // Push done in background for now, do not guarantee update before command return.
    secondDatabase.activateOnCurrentThread();
    BaseMemoryDatabase.assertWithTimeout(
        secondDatabase,
        () -> {
          secondDatabase.activateOnCurrentThread();
          assertNotNull(secondDatabase.getMetadata().getFunctionLibrary().getFunction("test"));
        });
  }

  @Test
  public void testSequencesUpdate() throws Exception {
    database.activateOnCurrentThread();
    database.begin();
    database.command("CREATE SEQUENCE test TYPE CACHED");
    database.commit();
    // Push done in background for now, do not guarantee update before command return.
    secondDatabase.activateOnCurrentThread();
    BaseMemoryDatabase.assertWithTimeout(
        secondDatabase,
        () -> {
          secondDatabase.activateOnCurrentThread();
          assertNotNull(secondDatabase.getMetadata().getSequenceLibrary().getSequence("test"));
        });
  }
}
