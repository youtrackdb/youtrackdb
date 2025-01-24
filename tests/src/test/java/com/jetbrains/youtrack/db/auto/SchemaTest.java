/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.ClusterDoesNotExistException;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityShared;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SchemaTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SchemaTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE,
        "EXCEPTION");
    return builder.build();
  }

  @Test
  public void checkSchema() {
    Schema schema = db.getMetadata().getSchema();

    assert schema != null;
    assert schema.getClass("Profile") != null;
    assert schema.getClass("Profile").getProperty("nick").getType() == PropertyType.STRING;
    assert schema.getClass("Profile").getProperty("name").getType() == PropertyType.STRING;
    assert schema.getClass("Profile").getProperty("surname").getType() == PropertyType.STRING;
    assert
        schema.getClass("Profile").getProperty("registeredOn").getType() == PropertyType.DATETIME;
    assert
        schema.getClass("Profile").getProperty("lastAccessOn").getType() == PropertyType.DATETIME;

    assert schema.getClass("Whiz") != null;
    assert schema.getClass("whiz").getProperty("account").getType() == PropertyType.LINK;
    assert schema
        .getClass("whiz")
        .getProperty("account")
        .getLinkedClass()
        .getName()
        .equalsIgnoreCase("Account");
    assert schema.getClass("WHIZ").getProperty("date").getType() == PropertyType.DATE;
    assert schema.getClass("WHIZ").getProperty("text").getType() == PropertyType.STRING;
    assert schema.getClass("WHIZ").getProperty("text").isMandatory();
    assert schema.getClass("WHIZ").getProperty("text").getMin().equals("1");
    assert schema.getClass("WHIZ").getProperty("text").getMax().equals("140");
    assert schema.getClass("whiz").getProperty("replyTo").getType() == PropertyType.LINK;
    assert schema
        .getClass("Whiz")
        .getProperty("replyTo")
        .getLinkedClass()
        .getName()
        .equalsIgnoreCase("Account");
  }

  @Test(dependsOnMethods = "checkSchema")
  public void checkInvalidNamesBefore30() {

    Schema schema = db.getMetadata().getSchema();

    schema.createClass("TestInvalidName,");
    Assert.assertNotNull(schema.getClass("TestInvalidName,"));
    schema.createClass("TestInvalidName;");
    Assert.assertNotNull(schema.getClass("TestInvalidName;"));
    schema.createClass("TestInvalid Name");
    Assert.assertNotNull(schema.getClass("TestInvalid Name"));
    schema.createClass("TestInvalid_Name");
    Assert.assertNotNull(schema.getClass("TestInvalid_Name"));
  }

  @Test(dependsOnMethods = "checkSchema")
  public void checkSchemaApi() {

    Schema schema = db.getMetadata().getSchema();

    try {
      Assert.assertNull(schema.getClass("Animal33"));
    } catch (SchemaException e) {
    }
  }

  @Test(dependsOnMethods = "checkSchemaApi")
  public void checkClusters() {

    for (SchemaClass cls : db.getMetadata().getSchema().getClasses()) {
      assert cls.isAbstract() || db.getClusterNameById(cls.getClusterIds()[0]) != null;
    }
  }

  @Test
  public void checkTotalRecords() {

    Assert.assertTrue(db.getStorage().countRecords(db) > 0);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void checkErrorOnUserNoPasswd() {
    db.begin();
    db.getMetadata().getSecurity().createUser("error", null, (String) null);
    db.commit();
  }

  @Test
  public void testMultiThreadSchemaCreation() throws InterruptedException {

    Thread thread =
        new Thread(
            new Runnable() {

              @Override
              public void run() {
                DatabaseRecordThreadLocal.instance().set(db);
                EntityImpl doc = ((EntityImpl) db.newEntity("NewClass"));

                db.begin();
                db.save(doc);
                db.commit();

                db.begin();
                doc = db.bindToSession(doc);
                doc.delete();
                db.commit();

                db.getMetadata().getSchema().dropClass("NewClass");
              }
            });

    thread.start();
    thread.join();
  }

  @Test
  public void createAndDropClassTestApi() {

    final String testClassName = "dropTestClass";
    final int clusterId;
    SchemaClass dropTestClass = db.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getClusterIds()[0];
    dropTestClass = db.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(db.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(db.getClusterNameById(clusterId));

    dropTestClass = db.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(db.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(db.getClusterNameById(clusterId));
    db.getMetadata().getSchema().dropClass(testClassName);
    dropTestClass = db.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(db.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(db.getClusterNameById(clusterId));

    dropTestClass = db.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(db.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(db.getClusterNameById(clusterId));
  }

  @Test
  public void createAndDropClassTestCommand() {

    final String testClassName = "dropTestClass";
    final int clusterId;
    SchemaClass dropTestClass = db.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getClusterIds()[0];
    dropTestClass = db.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(db.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(db.getClusterNameById(clusterId));

    dropTestClass = db.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(db.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(db.getClusterNameById(clusterId));
    db.command("drop class " + testClassName).close();

    dropTestClass = db.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(db.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(db.getClusterNameById(clusterId));

    dropTestClass = db.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(db.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(db.getClusterNameById(clusterId));
  }

  @Test
  public void customAttributes() {

    // TEST CUSTOM PROPERTY CREATION
    db
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom(db, "stereotype", "icon");

    Assert.assertEquals(
        db
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "icon");

    // TEST CUSTOM PROPERTY EXISTS EVEN AFTER REOPEN

    Assert.assertEquals(
        db
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "icon");

    // TEST CUSTOM PROPERTY REMOVAL
    db
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom(db, "stereotype", null);
    Assert.assertNull(
        db
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"));

    // TEST CUSTOM PROPERTY UPDATE
    db
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom(db, "stereotype", "polygon");
    Assert.assertEquals(
        db
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "polygon");

    // TEST CUSTOM PROPERTY UDPATED EVEN AFTER REOPEN

    Assert.assertEquals(
        db
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "polygon");

    // TEST CUSTOM PROPERTY WITH =

    db
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom(db, "equal", "this = that");

    Assert.assertEquals(
        db
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("equal"),
        "this = that");

    // TEST CUSTOM PROPERTY WITH = AFTER REOPEN

    Assert.assertEquals(
        db
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("equal"),
        "this = that");
  }

  @Test
  public void alterAttributes() {

    SchemaClass company = db.getMetadata().getSchema().getClass("Company");
    SchemaClass superClass = company.getSuperClass();

    Assert.assertNotNull(superClass);
    boolean found = false;
    for (SchemaClass c : superClass.getSubclasses()) {
      if (c.equals(company)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);

    company.setSuperClass(db, null);
    Assert.assertNull(company.getSuperClass());
    for (SchemaClass c : superClass.getSubclasses()) {
      Assert.assertNotSame(c, company);
    }

    db
        .command("alter class " + company.getName() + " superclass " + superClass.getName())
        .close();

    company = db.getMetadata().getSchema().getClass("Company");
    superClass = company.getSuperClass();

    Assert.assertNotNull(company.getSuperClass());
    found = false;
    for (SchemaClass c : superClass.getSubclasses()) {
      if (c.equals(company)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);
  }

  @Test
  public void invalidClusterWrongClusterId() {

    try {
      db.command(new CommandSQL("create class Antani cluster 212121")).execute(db);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof ClusterDoesNotExistException);
    }
  }

  @Test
  public void invalidClusterWrongClusterName() {
    try {
      db.command(new CommandSQL("create class Antani cluster blaaa")).execute(db);
      Assert.fail();

    } catch (Exception e) {
      Assert.assertTrue(e instanceof CommandSQLParsingException);
    }
  }

  @Test
  public void invalidClusterWrongKeywords() {

    try {
      db.command(new CommandSQL("create class Antani the pen is on the table"))
          .execute(db);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof CommandSQLParsingException);
    }
  }

  @Test
  public void testRenameClass() {

    var oClass = (SchemaClassInternal) db.getMetadata().getSchema()
        .createClass("RenameClassTest");

    db.begin();
    EntityImpl document = ((EntityImpl) db.newEntity("RenameClassTest"));
    document.save();

    document = ((EntityImpl) db.newEntity("RenameClassTest"));

    document.save();
    db.commit();

    db.begin();
    ResultSet result = db.query("select from RenameClassTest");
    Assert.assertEquals(result.stream().count(), 2);
    db.commit();

    oClass.set(db, SchemaClass.ATTRIBUTES.NAME, "RenameClassTest2");

    db.begin();
    result = db.query("select from RenameClassTest2");
    Assert.assertEquals(result.stream().count(), 2);
    db.commit();
  }

  public void testMinimumClustersAndClusterSelection() {
    db.command(new CommandSQL("alter database minimum_clusters 3")).execute(db);
    try {
      db.command("create class multipleclusters").close();

      Assert.assertTrue(db.existsCluster("multipleclusters"));

      for (int i = 1; i < 3; ++i) {
        Assert.assertTrue(db.existsCluster("multipleclusters_" + i));
      }

      for (int i = 0; i < 6; ++i) {
        db.begin();
        ((EntityImpl) db.newEntity("multipleclusters")).field("num", i).save();
        db.commit();
      }

      // CHECK THERE ARE 2 RECORDS IN EACH CLUSTER (ROUND-ROBIN STRATEGY)
      Assert.assertEquals(
          db.countClusterElements(db.getClusterIdByName("multipleclusters")), 2);
      for (int i = 1; i < 3; ++i) {
        Assert.assertEquals(
            db.countClusterElements(db.getClusterIdByName("multipleclusters_" + i)), 2);
      }

      db.begin();
      // DELETE ALL THE RECORDS
      var deleted =
          db.command("delete from cluster:multipleclusters_2").stream().
              findFirst().orElseThrow().<Long>getProperty("count");
      db.commit();
      Assert.assertEquals(deleted, 2);

      // CHANGE CLASS STRATEGY to BALANCED
      db
          .command("alter class multipleclusters cluster_selection balanced").close();

      for (int i = 0; i < 2; ++i) {
        db.begin();
        ((EntityImpl) db.newEntity("multipleclusters")).field("num", i).save();
        db.commit();
      }

      Assert.assertEquals(
          db.countClusterElements(db.getClusterIdByName("multipleclusters_2")), 2);

    } finally {
      // RESTORE DEFAULT
      db.command("alter database minimum_clusters 0").close();
    }
  }

  public void testExchangeCluster() {
    db.command("CREATE CLASS TestRenameClusterOriginal clusters 2").close();
    swapClusters(db, 1);
    swapClusters(db, 2);
    swapClusters(db, 3);
  }

  public void testRenameWithSameNameIsNop() {
    db.getMetadata().getSchema().getClass("V").setName(db, "V");
  }

  public void testRenameWithExistentName() {
    try {
      db.getMetadata().getSchema().getClass("V").setName(db, "OUser");
      Assert.fail();
    } catch (SchemaException e) {
    } catch (CommandExecutionException e) {
    }
  }

  public void testShortNameAlreadyExists() {
    try {
      db.getMetadata().getSchema().getClass("V").setShortName(db, "OUser");
      Assert.fail();
    } catch (IllegalArgumentException e) {
    } catch (CommandExecutionException e) {
    }
  }

  @Test
  public void testDeletionOfDependentClass() {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass oRestricted = schema.getClass(SecurityShared.RESTRICTED_CLASSNAME);
    SchemaClass classA = schema.createClass("TestDeletionOfDependentClassA", oRestricted);
    SchemaClass classB = schema.createClass("TestDeletionOfDependentClassB", classA);
    schema.dropClass(classB.getName());
  }

  @Test
  public void testCaseSensitivePropNames() {
    String className = "TestCaseSensitivePropNames";
    String propertyName = "propName";
    db.command("create class " + className);
    db.command(
        "create property "
            + className
            + "."
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " STRING");
    db.command(
        "create property "
            + className
            + "."
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " STRING");

    db.command(
        "create index "
            + className
            + "."
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " on "
            + className
            + "("
            + propertyName.toLowerCase(Locale.ENGLISH)
            + ") NOTUNIQUE");
    db.command(
        "create index "
            + className
            + "."
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " on "
            + className
            + "("
            + propertyName.toUpperCase(Locale.ENGLISH)
            + ") NOTUNIQUE");

    db.begin();
    db.command(
        "insert into "
            + className
            + " set "
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " = 'FOO', "
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " = 'foo'");
    db.command(
        "insert into "
            + className
            + " set "
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " = 'BAR', "
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " = 'bar'");
    db.commit();

    db.begin();
    try (ResultSet rs =
        db.command(
            "select from "
                + className
                + " where "
                + propertyName.toLowerCase(Locale.ENGLISH)
                + " = 'foo'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }
    db.commit();

    try (ResultSet rs =
        db.command(
            "select from "
                + className
                + " where "
                + propertyName.toLowerCase(Locale.ENGLISH)
                + " = 'FOO'")) {
      Assert.assertFalse(rs.hasNext());
    }

    db.begin();
    try (ResultSet rs =
        db.command(
            "select from "
                + className
                + " where "
                + propertyName.toUpperCase(Locale.ENGLISH)
                + " = 'FOO'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }
    db.commit();

    db.begin();
    try (ResultSet rs =
        db.command(
            "select from "
                + className
                + " where "
                + propertyName.toUpperCase(Locale.ENGLISH)
                + " = 'foo'")) {
      Assert.assertFalse(rs.hasNext());
    }
    db.commit();

    var schema = (SchemaInternal) db.getSchema();
    if (!remoteDB) {
      var clazz = schema.getClassInternal(className);
      var idx = clazz.getIndexesInternal(db);

      Set<String> indexes = new HashSet<>();
      for (Index id : idx) {
        indexes.add(id.getName());
      }

      Assert.assertTrue(
          indexes.contains(className + "." + propertyName.toLowerCase(Locale.ENGLISH)));
      Assert.assertTrue(
          indexes.contains(className + "." + propertyName.toUpperCase(Locale.ENGLISH)));
    }

    schema.dropClass(className);
  }

  private void swapClusters(DatabaseSessionInternal databaseDocumentTx, int i) {
    databaseDocumentTx
        .command("CREATE CLASS TestRenameClusterNew extends TestRenameClusterOriginal clusters 2")
        .close();
    databaseDocumentTx.begin();
    databaseDocumentTx
        .command("INSERT INTO TestRenameClusterNew (iteration) VALUES(" + i + ")")
        .close();
    databaseDocumentTx.commit();

    databaseDocumentTx
        .command("ALTER CLASS TestRenameClusterOriginal remove_cluster TestRenameClusterOriginal")
        .close();
    databaseDocumentTx
        .command("ALTER CLASS TestRenameClusterNew remove_cluster TestRenameClusterNew")
        .close();
    databaseDocumentTx.command("DROP CLASS TestRenameClusterNew").close();
    databaseDocumentTx
        .command("ALTER CLASS TestRenameClusterOriginal add_cluster TestRenameClusterNew")
        .close();
    databaseDocumentTx.command("DROP CLUSTER TestRenameClusterOriginal").close();
    databaseDocumentTx
        .command(
            new CommandSQL("ALTER CLUSTER TestRenameClusterNew name TestRenameClusterOriginal"))
        .execute(db);

    db.begin();
    List<EntityImpl> result =
        databaseDocumentTx.query(
            new SQLSynchQuery<EntityImpl>("select * from TestRenameClusterOriginal"));
    Assert.assertEquals(result.size(), 1);

    EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Object>field("iteration"), i);
    db.commit();
  }
}
