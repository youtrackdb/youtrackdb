package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
@Test
public abstract class BaseTest<T extends ODatabase> {

  private static final boolean keepDatabase = Boolean.getBoolean("orientdb.test.keepDatabase");

  public static String prepareUrl(String url) {
    if (url != null) return url;

    String storageType;
    final String config = System.getProperty("orientdb.test.env");
    if ("ci".equals(config) || "release".equals(config)) storageType = "plocal";
    else storageType = System.getProperty("storageType");

    if (storageType == null) storageType = "memory";

    if ("remote".equals(storageType)) return storageType + ":localhost/demo";
    else {
      final String buildDirectory = System.getProperty("buildDirectory", ".");
      return storageType + ":" + buildDirectory + "/test-db/demo";
    }
  }

  protected T database;
  protected OrientDB orientDB;

  protected String url;
  private boolean dropDb = false;

  protected String dbName;
  private ODatabaseType storageType;
  private boolean autoManageDatabase = true;

  protected BaseTest() {}

  @Parameters(value = "url")
  public BaseTest(@Optional String url) {
    this(url, null);
  }

  @Parameters(value = "url")
  public BaseTest(@Optional String url, String prefix) {
    String config = System.getProperty("orientdb.test.env");
    String remote = System.getProperty("orientdb.test.remote");

    if ("ci".equals(config) || "release".equals(config)) {
      storageType = ODatabaseType.PLOCAL;
    } else {
      var storageProperty = System.getProperty("storageType");
      if (storageProperty != null) {
        storageType = ODatabaseType.valueOf(storageProperty.toUpperCase(Locale.ROOT));
      } else {
        storageType = ODatabaseType.MEMORY;
      }
    }

    if (url == null) {
      if (remote != null) {
        url = storageType + ":localhost/demo";
        dropDb = !keepDatabase;
      } else {
        final String buildDirectory = System.getProperty("buildDirectory", ".");
        url =
            storageType.toString().toLowerCase(Locale.ROOT)
                + ":"
                + buildDirectory
                + "/test-db/demo";
        dropDb = !keepDatabase;
      }
    }

    if (prefix == null) {
      dbName = "demo";
    } else {
      dbName = prefix + "demo";
    }

    this.url = url;
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    var dbConfig =
        OrientDBConfig.builder().addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, true);
    orientDB = new OrientDB(url, dbConfig.build());

    if (!url.startsWith("remote:")) {
      if (!orientDB.exists(dbName)) {
        orientDB.create(dbName, storageType);
      }
    }

    if (dropDb) {
      if (orientDB.exists(dbName)) {
        orientDB.drop(dbName);
      }

      orientDB.createIfNotExists(dbName, storageType);
    }

    database = createDatabaseSession();
    this.url = database.getURL();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (!autoManageDatabase) return;

    if (dropDb) {
      if (orientDB.exists(dbName)) {
        orientDB.drop(dbName);
      }
    } else {
      if (!database.isClosed()) {
        database.activateOnCurrentThread();
        database.close();
      }
    }
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    if (!autoManageDatabase) return;
    database.activateOnCurrentThread();
    if (database.isClosed()) {
      database = createDatabaseSession();
    }
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    if (!autoManageDatabase) return;

    if (!database.isClosed()) {
      database.activateOnCurrentThread();
      database.close();
    }
  }

  protected abstract T createDatabaseSession();

  protected abstract  T createDatabaseSession(String user, String password);

  protected void createDatabase() throws IOException {
    ODatabaseHelper.createDatabase(database, database.getURL());
  }

  protected static String getTestEnv() {
    return System.getProperty("orientdb.test.env");
  }

  protected final String getStorageType() {
    return storageType.name().toLowerCase(Locale.ROOT);
  }

  protected void createBasicTestSchema() {
    ODatabase database = this.database;
    if (database instanceof OObjectDatabaseTx)
      database = ((OObjectDatabaseTx) database).getUnderlying();

    if (database.getMetadata().getSchema().existsClass("Whiz")) return;

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

  protected void dropDb() {
    if (orientDB.exists(dbName)) {
      orientDB.drop(dbName);
    }
  }

  protected void createDb() {
    orientDB.createIfNotExists(dbName, storageType);
  }

  @SuppressWarnings("SameParameterValue")
  protected void setAutoManageDatabase(final boolean autoManageDatabase) {
    this.autoManageDatabase = autoManageDatabase;
  }

  @SuppressWarnings("SameParameterValue")
  protected void setDropDb(final boolean dropDb) {
    this.dropDb = !keepDatabase && dropDb;
  }

  protected void checkEmbeddedDB() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is running only in embedded database");
    }
  }

  protected OIndex getIndex(final String indexName) {
    final ODatabaseDocumentInternal db;
    if (database instanceof ODatabaseWrapperAbstract) {
      db = (ODatabaseDocumentInternal) ((ODatabaseWrapperAbstract) database).getUnderlying();
    } else {
      db = (ODatabaseDocumentInternal) database;
    }
    //noinspection unchecked
    return (OIndex) (db.getMetadata()).getIndexManagerInternal().getIndex(db, indexName);
  }
}
