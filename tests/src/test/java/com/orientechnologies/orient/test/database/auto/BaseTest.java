package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.db.OObjectDatabaseTxInternal;
import com.orientechnologies.orient.server.OServer;
import java.util.Locale;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public abstract class BaseTest<T extends ODatabaseInternal<?>> {

  private OServer server;

  public static final String DEFAULT_DB_NAME = "demo";

  protected T database;
  protected String dbName;

  protected boolean remoteDB = false;
  protected ODatabaseType databaseType;

  public static OrientDB orientDB;

  protected BaseTest() {}

  @Parameters(value = "remote")
  public BaseTest(boolean remote) {
    String config = System.getProperty("orientdb.test.env");

    if ("ci".equals(config) || "release".equals(config)) {
      databaseType = ODatabaseType.PLOCAL;
    }

    if (databaseType == null) {
      databaseType = ODatabaseType.MEMORY;
    }

    this.remoteDB = remote;

    this.dbName = DEFAULT_DB_NAME;
  }

  @Parameters(value = "remote")
  public BaseTest(boolean remote, String prefix) {
    this(remote);
    this.dbName = prefix + DEFAULT_DB_NAME;
  }

  @BeforeSuite
  public void beforeSuite() {
    try {
      if (remoteDB && server == null) {
        server = new OServer(false);
        server.startup(
            BaseTest.class.getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
        server.activate();
      }

      if (orientDB == null) {
        var builder = new OrientDBConfigBuilder();
        if (remoteDB) {
          orientDB =
              new OrientDB(
                  "remote:localhost",
                  "root",
                  "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3",
                  createConfig(builder));
        } else {
          final String buildDirectory = System.getProperty("buildDirectory", ".");
          orientDB = OrientDB.embedded(buildDirectory + "/test-db", createConfig(builder));
        }
      }

      createDatabase();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Cannot create database in test " + this.getClass().getSimpleName(), e);
    }
  }

  protected void createDatabase(String dbName) {
    orientDB.createIfNotExists(
        dbName,
        databaseType,
        "admin",
        "admin",
        "admin",
        "writer",
        "writer",
        "writer",
        "reader",
        "reader",
        "reader");
  }

  protected void createDatabase() {
    createDatabase(dbName);
  }

  protected void dropDatabase(String dbName) {
    if (orientDB.exists(dbName)) {
      orientDB.drop(dbName);
    }
  }

  @AfterSuite
  public void afterSuite() {
    try {
      if (orientDB != null) {
        orientDB.close();
        orientDB = null;
      }

      if (remoteDB && server != null) {
        server.shutdown();
        server = null;
      }
    } catch (Exception e) {
      throw new IllegalStateException(
          "Cannot close database instance in test " + this.getClass().getSimpleName(), e);
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    createDatabase();
    newSession();
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    newSession();
  }

  private void newSession() {
    try {
      if (database == null) {
        database = createSessionInstance();
      }

      database.activateOnCurrentThread();
      if (database.isClosed()) {
        database = createSessionInstance();
      }
    } catch (Exception e) {
      throw new IllegalStateException(
          "Cannot open database session in test " + this.getClass().getSimpleName(), e);
    }
  }

  @AfterClass
  public void afterClass() throws Exception {
    closeSession();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    closeSession();
  }

  private void closeSession() {
    try {
      if (!database.isClosed()) {
        database.activateOnCurrentThread();
        database.close();
      }
    } catch (Exception e) {
      throw new IllegalStateException(
          "Cannot close database session in test " + this.getClass().getSimpleName(), e);
    }
  }

  protected abstract T createSessionInstance(
      OrientDB orientDB, String dbName, String user, String password);

  protected final T createSessionInstance() {
    return createSessionInstance("admin", "admin");
  }

  protected final T createSessionInstance(String dbName) {
    return createSessionInstance(dbName, "admin", "admin");
  }

  protected final T createSessionInstance(String dbName, String user, String password) {
    return createSessionInstance(orientDB, dbName, user, password);
  }

  protected final T createSessionInstance(String user, String password) {
    return createSessionInstance(dbName, user, password);
  }

  protected ODatabaseSessionInternal acquireSession() {
    return acquireSession(dbName);
  }

  protected ODatabaseSessionInternal acquireSession(String dbName) {
    return (ODatabaseSessionInternal) orientDB.open(dbName, "admin", "admin");
  }

  protected OrientDBConfig createConfig(OrientDBConfigBuilder builder) {
    builder.addConfig(OGlobalConfiguration.NON_TX_READS_WARNING_MODE, "SILENT");
    return builder.build();
  }

  protected static String getTestEnv() {
    return System.getProperty("orientdb.test.env");
  }

  protected final String getStorageType() {
    return databaseType.toString().toLowerCase(Locale.ROOT);
  }

  protected void createBasicTestSchema() {
    ODatabaseInternal<?> database = this.database;
    if (database instanceof OObjectDatabaseTxInternal) {
      database = database.getUnderlying();
    }

    if (database.getMetadata().getSchema().existsClass("Whiz")) {
      return;
    }

    database.addCluster("csv");
    database.addCluster("flat");
    database.addCluster("binary");

    OClass account = database.getMetadata().getSchema().createClass("Account", 1, (OClass[]) null);
    account.createProperty("id", OType.INTEGER);
    account.createProperty("birthDate", OType.DATE);
    account.createProperty("binary", OType.BINARY);

    database.getMetadata().getSchema().createClass("Company", account);

    OClass profile = database.getMetadata().getSchema().createClass("Profile", 1, (OClass[]) null);
    profile
        .createProperty("nick", OType.STRING)
        .setMin("3")
        .setMax("30")
        .createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));
    profile
        .createProperty("name", OType.STRING)
        .setMin("3")
        .setMax("30")
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    profile.createProperty("surname", OType.STRING).setMin("3").setMax("30");
    profile.createProperty("registeredOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
    profile.createProperty("lastAccessOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
    profile.createProperty("photo", OType.TRANSIENT);

    OClass whiz = database.getMetadata().getSchema().createClass("Whiz", 1, (OClass[]) null);
    whiz.createProperty("id", OType.INTEGER);
    whiz.createProperty("account", OType.LINK, account);
    whiz.createProperty("date", OType.DATE).setMin("2010-01-01");
    whiz.createProperty("text", OType.STRING).setMandatory(true).setMin("1").setMax("140");
    whiz.createProperty("replyTo", OType.LINK, account);

    OClass strictTest =
        database.getMetadata().getSchema().createClass("StrictTest", 1, (OClass[]) null);
    strictTest.setStrictMode(true);
    strictTest.createProperty("id", OType.INTEGER).isMandatory();
    strictTest.createProperty("name", OType.STRING);

    OClass animalRace =
        database.getMetadata().getSchema().createClass("AnimalRace", 1, (OClass[]) null);
    animalRace.createProperty("name", OType.STRING);

    OClass animal = database.getMetadata().getSchema().createClass("Animal", 1, (OClass[]) null);
    animal.createProperty("races", OType.LINKSET, animalRace);
    animal.createProperty("name", OType.STRING);
  }

  protected void checkEmbeddedDB() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is running only in embedded database");
    }
  }

  protected OIndex getIndex(final String indexName) {
    final ODatabaseSessionInternal db;
    if (database instanceof ODatabaseWrapperAbstract) {
      db = database.getUnderlying();
    } else {
      db = (ODatabaseSessionInternal) database;
    }

    return (db.getMetadata()).getIndexManagerInternal().getIndex(db, indexName);
  }
}
