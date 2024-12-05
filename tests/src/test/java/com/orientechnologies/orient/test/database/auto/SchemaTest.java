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
package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilder;
import com.jetbrains.youtrack.db.internal.core.exception.YTClusterDoesNotExistException;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.exception.YTSchemaException;
import com.jetbrains.youtrack.db.internal.core.exception.YTValidationException;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityShared;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.OCommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.YTCommandSQLParsingException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLSynchQuery;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SchemaTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public SchemaTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(GlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
    return builder.build();
  }

  @Test
  public void checkSchema() {
    YTSchema schema = database.getMetadata().getSchema();

    assert schema != null;
    assert schema.getClass("Profile") != null;
    assert schema.getClass("Profile").getProperty("nick").getType() == YTType.STRING;
    assert schema.getClass("Profile").getProperty("name").getType() == YTType.STRING;
    assert schema.getClass("Profile").getProperty("surname").getType() == YTType.STRING;
    assert schema.getClass("Profile").getProperty("registeredOn").getType() == YTType.DATETIME;
    assert schema.getClass("Profile").getProperty("lastAccessOn").getType() == YTType.DATETIME;

    assert schema.getClass("Whiz") != null;
    assert schema.getClass("whiz").getProperty("account").getType() == YTType.LINK;
    assert schema
        .getClass("whiz")
        .getProperty("account")
        .getLinkedClass()
        .getName()
        .equalsIgnoreCase("Account");
    assert schema.getClass("WHIZ").getProperty("date").getType() == YTType.DATE;
    assert schema.getClass("WHIZ").getProperty("text").getType() == YTType.STRING;
    assert schema.getClass("WHIZ").getProperty("text").isMandatory();
    assert schema.getClass("WHIZ").getProperty("text").getMin().equals("1");
    assert schema.getClass("WHIZ").getProperty("text").getMax().equals("140");
    assert schema.getClass("whiz").getProperty("replyTo").getType() == YTType.LINK;
    assert schema
        .getClass("Whiz")
        .getProperty("replyTo")
        .getLinkedClass()
        .getName()
        .equalsIgnoreCase("Account");
  }

  @Test(dependsOnMethods = "checkSchema")
  public void checkInvalidNamesBefore30() {

    YTSchema schema = database.getMetadata().getSchema();

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

    YTSchema schema = database.getMetadata().getSchema();

    try {
      Assert.assertNull(schema.getClass("Animal33"));
    } catch (YTSchemaException e) {
    }
  }

  @Test(dependsOnMethods = "checkSchemaApi")
  public void checkClusters() {

    for (YTClass cls : database.getMetadata().getSchema().getClasses()) {
      assert cls.isAbstract() || database.getClusterNameById(cls.getDefaultClusterId()) != null;
    }
  }

  @Test
  public void checkTotalRecords() {

    Assert.assertTrue(database.getStorage().countRecords(database) > 0);
  }

  @Test(expectedExceptions = YTValidationException.class)
  public void checkErrorOnUserNoPasswd() {
    database.begin();
    database.getMetadata().getSecurity().createUser("error", null, (String) null);
    database.commit();
  }

  @Test
  public void testMultiThreadSchemaCreation() throws InterruptedException {

    Thread thread =
        new Thread(
            new Runnable() {

              @Override
              public void run() {
                ODatabaseRecordThreadLocal.instance().set(database);
                EntityImpl doc = new EntityImpl("NewClass");

                database.begin();
                database.save(doc);
                database.commit();

                database.begin();
                doc = database.bindToSession(doc);
                doc.delete();
                database.commit();

                database.getMetadata().getSchema().dropClass("NewClass");
              }
            });

    thread.start();
    thread.join();
  }

  @Test
  public void createAndDropClassTestApi() {

    final String testClassName = "dropTestClass";
    final int clusterId;
    YTClass dropTestClass = database.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getDefaultClusterId();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));
    database.getMetadata().getSchema().dropClass(testClassName);
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));
  }

  @Test
  public void createAndDropClassTestCommand() {

    final String testClassName = "dropTestClass";
    final int clusterId;
    YTClass dropTestClass = database.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getDefaultClusterId();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));
    database.command("drop class " + testClassName).close();

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));
  }

  @Test
  public void customAttributes() {

    // TEST CUSTOM PROPERTY CREATION
    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom(database, "stereotype", "icon");

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "icon");

    // TEST CUSTOM PROPERTY EXISTS EVEN AFTER REOPEN

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "icon");

    // TEST CUSTOM PROPERTY REMOVAL
    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom(database, "stereotype", null);
    Assert.assertNull(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"));

    // TEST CUSTOM PROPERTY UPDATE
    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom(database, "stereotype", "polygon");
    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "polygon");

    // TEST CUSTOM PROPERTY UDPATED EVEN AFTER REOPEN

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("stereotype"),
        "polygon");

    // TEST CUSTOM PROPERTY WITH =

    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .setCustom(database, "equal", "this = that");

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("equal"),
        "this = that");

    // TEST CUSTOM PROPERTY WITH = AFTER REOPEN

    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("Profile")
            .getProperty("nick")
            .getCustom("equal"),
        "this = that");
  }

  @Test
  public void alterAttributes() {

    YTClass company = database.getMetadata().getSchema().getClass("Company");
    YTClass superClass = company.getSuperClass();

    Assert.assertNotNull(superClass);
    boolean found = false;
    for (YTClass c : superClass.getSubclasses()) {
      if (c.equals(company)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);

    company.setSuperClass(database, null);
    Assert.assertNull(company.getSuperClass());
    for (YTClass c : superClass.getSubclasses()) {
      Assert.assertNotSame(c, company);
    }

    database
        .command("alter class " + company.getName() + " superclass " + superClass.getName())
        .close();

    company = database.getMetadata().getSchema().getClass("Company");
    superClass = company.getSuperClass();

    Assert.assertNotNull(company.getSuperClass());
    found = false;
    for (YTClass c : superClass.getSubclasses()) {
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
      database.command(new OCommandSQL("create class Antani cluster 212121")).execute(database);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof YTClusterDoesNotExistException);
    }
  }

  @Test
  public void invalidClusterWrongClusterName() {
    try {
      database.command(new OCommandSQL("create class Antani cluster blaaa")).execute(database);
      Assert.fail();

    } catch (Exception e) {
      Assert.assertTrue(e instanceof YTCommandSQLParsingException);
    }
  }

  @Test
  public void invalidClusterWrongKeywords() {

    try {
      database.command(new OCommandSQL("create class Antani the pen is on the table"))
          .execute(database);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof YTCommandSQLParsingException);
    }
  }

  @Test
  public void testRenameClass() {

    YTClass oClass = database.getMetadata().getSchema().createClass("RenameClassTest");

    database.begin();
    EntityImpl document = new EntityImpl("RenameClassTest");
    document.save();

    document = new EntityImpl("RenameClassTest");

    document.setClassName("RenameClassTest");
    document.save();
    database.commit();

    database.begin();
    YTResultSet result = database.query("select from RenameClassTest");
    Assert.assertEquals(result.stream().count(), 2);
    database.commit();

    oClass.set(database, YTClass.ATTRIBUTES.NAME, "RenameClassTest2");

    database.begin();
    result = database.query("select from RenameClassTest2");
    Assert.assertEquals(result.stream().count(), 2);
    database.commit();
  }

  public void testMinimumClustersAndClusterSelection() {

    database.command(new OCommandSQL("alter database minimumclusters 3")).execute(database);

    try {
      database.command("create class multipleclusters").close();

      Assert.assertTrue(database.existsCluster("multipleclusters"));

      for (int i = 1; i < 3; ++i) {
        Assert.assertTrue(database.existsCluster("multipleclusters_" + i));
      }

      for (int i = 0; i < 6; ++i) {
        database.begin();
        new EntityImpl("multipleclusters").field("num", i).save();
        database.commit();
      }

      // CHECK THERE ARE 2 RECORDS IN EACH CLUSTER (ROUND-ROBIN STRATEGY)
      Assert.assertEquals(
          database.countClusterElements(database.getClusterIdByName("multipleclusters")), 2);
      for (int i = 1; i < 3; ++i) {
        Assert.assertEquals(
            database.countClusterElements(database.getClusterIdByName("multipleclusters_" + i)), 2);
      }

      database.begin();
      // DELETE ALL THE RECORDS
      var deleted =
          database.command("delete from cluster:multipleclusters_2").stream().
              findFirst().orElseThrow().<Long>getProperty("count");
      database.commit();
      Assert.assertEquals(deleted, 2);

      // CHANGE CLASS STRATEGY to BALANCED
      database
          .command("alter class multipleclusters clusterselection balanced").close();

      for (int i = 0; i < 2; ++i) {
        database.begin();
        new EntityImpl("multipleclusters").field("num", i).save();
        database.commit();
      }

      Assert.assertEquals(
          database.countClusterElements(database.getClusterIdByName("multipleclusters_2")), 2);

    } finally {
      // RESTORE DEFAULT
      database.command("alter database minimumclusters 0").close();
    }
  }

  public void testExchangeCluster() {
    database.command("CREATE CLASS TestRenameClusterOriginal clusters 2").close();
    swapClusters(database, 1);
    swapClusters(database, 2);
    swapClusters(database, 3);
  }

  public void testRenameWithSameNameIsNop() {
    database.getMetadata().getSchema().getClass("V").setName(database, "V");
  }

  public void testRenameWithExistentName() {
    try {
      database.getMetadata().getSchema().getClass("V").setName(database, "OUser");
      Assert.fail();
    } catch (YTSchemaException e) {
    } catch (YTCommandExecutionException e) {
    }
  }

  public void testShortNameAlreadyExists() {
    try {
      database.getMetadata().getSchema().getClass("V").setShortName(database, "OUser");
      Assert.fail();
    } catch (IllegalArgumentException e) {
    } catch (YTCommandExecutionException e) {
    }
  }

  @Test
  public void testDeletionOfDependentClass() {
    YTSchema schema = database.getMetadata().getSchema();
    YTClass oRestricted = schema.getClass(OSecurityShared.RESTRICTED_CLASSNAME);
    YTClass classA = schema.createClass("TestDeletionOfDependentClassA", oRestricted);
    YTClass classB = schema.createClass("TestDeletionOfDependentClassB", classA);
    schema.dropClass(classB.getName());
  }

  @Test
  public void testCaseSensitivePropNames() {
    String className = "TestCaseSensitivePropNames";
    String propertyName = "propName";
    database.command("create class " + className);
    database.command(
        "create property "
            + className
            + "."
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " STRING");
    database.command(
        "create property "
            + className
            + "."
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " STRING");

    database.command(
        "create index "
            + className
            + "."
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " on "
            + className
            + "("
            + propertyName.toLowerCase(Locale.ENGLISH)
            + ") NOTUNIQUE");
    database.command(
        "create index "
            + className
            + "."
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " on "
            + className
            + "("
            + propertyName.toUpperCase(Locale.ENGLISH)
            + ") NOTUNIQUE");

    database.begin();
    database.command(
        "insert into "
            + className
            + " set "
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " = 'FOO', "
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " = 'foo'");
    database.command(
        "insert into "
            + className
            + " set "
            + propertyName.toUpperCase(Locale.ENGLISH)
            + " = 'BAR', "
            + propertyName.toLowerCase(Locale.ENGLISH)
            + " = 'bar'");
    database.commit();

    database.begin();
    try (YTResultSet rs =
        database.command(
            "select from "
                + className
                + " where "
                + propertyName.toLowerCase(Locale.ENGLISH)
                + " = 'foo'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }
    database.commit();

    try (YTResultSet rs =
        database.command(
            "select from "
                + className
                + " where "
                + propertyName.toLowerCase(Locale.ENGLISH)
                + " = 'FOO'")) {
      Assert.assertFalse(rs.hasNext());
    }

    database.begin();
    try (YTResultSet rs =
        database.command(
            "select from "
                + className
                + " where "
                + propertyName.toUpperCase(Locale.ENGLISH)
                + " = 'FOO'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }
    database.commit();

    database.begin();
    try (YTResultSet rs =
        database.command(
            "select from "
                + className
                + " where "
                + propertyName.toUpperCase(Locale.ENGLISH)
                + " = 'foo'")) {
      Assert.assertFalse(rs.hasNext());
    }
    database.commit();

    YTSchema schema = database.getSchema();
    YTClass clazz = schema.getClass(className);
    Set<OIndex> idx = clazz.getIndexes(database);
    Set<String> indexes = new HashSet<>();
    for (OIndex id : idx) {
      indexes.add(id.getName());
    }
    Assert.assertTrue(indexes.contains(className + "." + propertyName.toLowerCase(Locale.ENGLISH)));
    Assert.assertTrue(indexes.contains(className + "." + propertyName.toUpperCase(Locale.ENGLISH)));
    schema.dropClass(className);
  }

  private void swapClusters(YTDatabaseSessionInternal databaseDocumentTx, int i) {
    databaseDocumentTx
        .command("CREATE CLASS TestRenameClusterNew extends TestRenameClusterOriginal clusters 2")
        .close();

    databaseDocumentTx.begin();
    databaseDocumentTx
        .command("INSERT INTO TestRenameClusterNew (iteration) VALUES(" + i + ")")
        .close();
    databaseDocumentTx.commit();

    databaseDocumentTx
        .command("ALTER CLASS TestRenameClusterOriginal removecluster TestRenameClusterOriginal")
        .close();
    databaseDocumentTx
        .command("ALTER CLASS TestRenameClusterNew removecluster TestRenameClusterNew")
        .close();
    databaseDocumentTx.command("DROP CLASS TestRenameClusterNew").close();
    databaseDocumentTx
        .command("ALTER CLASS TestRenameClusterOriginal addcluster TestRenameClusterNew")
        .close();
    databaseDocumentTx.command("DROP CLUSTER TestRenameClusterOriginal").close();
    databaseDocumentTx
        .command(
            new OCommandSQL("ALTER CLUSTER TestRenameClusterNew name TestRenameClusterOriginal"))
        .execute(database);

    database.begin();
    List<EntityImpl> result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<EntityImpl>("select * from TestRenameClusterOriginal"));
    Assert.assertEquals(result.size(), 1);

    EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Object>field("iteration"), i);
    database.commit();
  }
}
