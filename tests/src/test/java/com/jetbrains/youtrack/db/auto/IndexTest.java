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

import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrack.db.api.query.ExecutionStep;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.executor.FetchFromIndexStep;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import com.jetbrains.youtrack.db.internal.core.storage.cache.WriteCache;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@SuppressWarnings({"deprecation", "unchecked"})
@Test
public class IndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public IndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    generateCompanyData();
  }

  public void testDuplicatedIndexOnUnique() {
    var jayMiner = db.newEntity("Profile");
    jayMiner.setProperty("nick", "Jay");
    jayMiner.setProperty("name", "Jay");
    jayMiner.setProperty("surname", "Miner");

    db.begin();
    db.save(jayMiner);
    db.commit();

    var jacobMiner = db.newEntity("Profile");
    jacobMiner.setProperty("nick", "Jay");
    jacobMiner.setProperty("name", "Jacob");
    jacobMiner.setProperty("surname", "Miner");

    try {
      db.begin();
      db.save(jacobMiner);
      db.commit();

      // IT SHOULD GIVE ERROR ON DUPLICATED KEY
      Assert.fail();

    } catch (RecordDuplicatedException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInUniqueIndex() {
    checkEmbeddedDB();
    Assert.assertEquals(
        db.getMetadata().getSchema().getClassInternal("Profile")
            .getInvolvedIndexesInternal(db, "nick").iterator().next().getType(),
        SchemaClass.INDEX_TYPE.UNIQUE.toString());
    try (var resultSet =
        db.query(
            "SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0'"
                + " ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']")) {
      assertIndexUsage(resultSet);

      final List<String> expectedSurnames =
          new ArrayList<>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (final Entity profile : result) {
        expectedSurnames.remove(profile.<String>getProperty("surname"));
      }

      Assert.assertEquals(expectedSurnames.size(), 0);
    }
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
  public void testUseOfIndex() {
    final List<EntityImpl> result = executeQuery("select * from Profile where nick = 'Jay'");

    Assert.assertFalse(result.isEmpty());

    Entity record;
    for (EntityImpl entries : result) {
      record = entries;
      Assert.assertTrue(record.<String>getProperty("name").equalsIgnoreCase("Jay"));
    }
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
  public void testIndexEntries() {
    checkEmbeddedDB();

    List<EntityImpl> result = executeQuery("select * from Profile where nick is not null");

    Index idx =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "Profile.nick");

    Assert.assertEquals(idx.getInternal().size(db), result.size());
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
  public void testIndexSize() {
    checkEmbeddedDB();

    List<EntityImpl> result = executeQuery("select * from Profile where nick is not null");

    int profileSize = result.size();

    Assert.assertEquals(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "Profile.nick")
            .getInternal()
            .size(db),
        profileSize);
    for (int i = 0; i < 10; i++) {
      db.begin();
      Entity profile = db.newEntity("Profile");
      profile.setProperty("nick", "Yay-" + i);
      profile.setProperty("name", "Jay");
      profile.setProperty("surname", "Miner");
      db.save(profile);
      db.commit();

      profileSize++;
      try (Stream<RID> stream =
          db
              .getMetadata()
              .getIndexManagerInternal()
              .getIndex(db, "Profile.nick")
              .getInternal()
              .getRids(db, "Yay-" + i)) {
        Assert.assertTrue(stream.findAny().isPresent());
      }
    }
  }

  @Test(dependsOnMethods = "testUseOfIndex")
  public void testChangeOfIndexToNotUnique() {
    dropIndexes("Profile", "nick");

    db
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .createIndex(db, INDEX_TYPE.NOTUNIQUE);
  }

  private void dropIndexes(String className, String propertyName) {
    if (remoteDB) {
      db.command("drop index " + className + "." + propertyName).close();
    } else {
      for (var indexName : db.getMetadata().getSchema().getClassInternal(className)
          .getPropertyInternal(propertyName).getAllIndexes(db)) {
        db.getMetadata().getIndexManagerInternal().dropIndex(db, indexName);
      }
    }
  }

  @Test(dependsOnMethods = "testChangeOfIndexToNotUnique")
  public void testDuplicatedIndexOnNotUnique() {
    db.begin();
    Entity nickNolte = db.newEntity("Profile");
    nickNolte.setProperty("nick", "Jay");
    nickNolte.setProperty("name", "Nick");
    nickNolte.setProperty("surname", "Nolte");

    db.save(nickNolte);
    db.commit();
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnNotUnique")
  public void testChangeOfIndexToUnique() {
    try {
      dropIndexes("Profile", "nick");
      db
          .getMetadata()
          .getSchema()
          .getClass("Profile")
          .getProperty("nick")
          .createIndex(db, INDEX_TYPE.UNIQUE);
      Assert.fail();
    } catch (RecordDuplicatedException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMajorSelect() {
    if (db.isRemote()) {
      return;
    }

    try (var resultSet =
        db.query("select * from Profile where nick > 'ZZZJayLongNickIndex3'")) {
      assertIndexUsage(resultSet);
      final List<String> expectedNicks =
          new ArrayList<>(Arrays.asList("ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 2);
      for (Entity profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMajorEqualsSelect() {
    if (db.isRemote()) {
      return;
    }

    try (var resultSet =
        db.query("select * from Profile where nick >= 'ZZZJayLongNickIndex3'")) {
      assertIndexUsage(resultSet);
      final List<String> expectedNicks =
          new ArrayList<>(
              Arrays.asList(
                  "ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (Entity profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMinorSelect() {
    if (db.isRemote()) {
      return;
    }

    try (var resultSet = db.query("select * from Profile where nick < '002'")) {
      assertIndexUsage(resultSet);
      final List<String> expectedNicks = new ArrayList<>(Arrays.asList("000", "001"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 2);
      for (Entity profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMinorEqualsSelect() {
    if (db.isRemote()) {
      return;
    }

    try (var resultSet = db.query("select * from Profile where nick <= '002'")) {
      final List<String> expectedNicks = new ArrayList<>(Arrays.asList("000", "001", "002"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (Entity profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments", enabled = false)
  public void testIndexBetweenSelect() {
    if (db.isRemote()) {
      return;
    }

    var query = "select * from Profile where nick between '001' and '004'";
    try (var resultSet = db.query(query)) {
      assertIndexUsage(resultSet);
      final List<String> expectedNicks = new ArrayList<>(Arrays.asList("001", "002", "003", "004"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 4);
      for (Entity profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments", enabled = false)
  public void testIndexInComplexSelectOne() {
    if (db.isRemote()) {
      return;
    }

    try (var resultSet =
        db.query(
            "select * from Profile where (name = 'Giuseppe' OR name <> 'Napoleone') AND"
                + " (nick is not null AND (name = 'Giuseppe' OR name <> 'Napoleone') AND"
                + " (nick >= 'ZZZJayLongNickIndex3'))")) {
      assertIndexUsage(resultSet);

      final List<String> expectedNicks =
          new ArrayList<>(
              Arrays.asList(
                  "ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (Entity profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments", enabled = false)
  public void testIndexInComplexSelectTwo() {
    if (db.isRemote()) {
      return;
    }

    try (var resultSet =
        db.query(
            "select * from Profile where ((name = 'Giuseppe' OR name <> 'Napoleone') AND"
                + " (nick is not null AND (name = 'Giuseppe' OR name <> 'Napoleone') AND"
                + " (nick >= 'ZZZJayLongNickIndex3' OR nick >= 'ZZZJayLongNickIndex4')))")) {
      assertIndexUsage(resultSet);

      final List<String> expectedNicks =
          new ArrayList<>(
              Arrays.asList(
                  "ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));
      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (Entity profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  public void populateIndexDocuments() {
    for (int i = 0; i <= 5; i++) {
      db.begin();
      final Entity profile = db.newEntity("Profile");
      profile.setProperty("nick", "ZZZJayLongNickIndex" + i);
      profile.setProperty("name", "NickIndex" + i);
      profile.setProperty("surname", "NolteIndex" + i);
      db.save(profile);
      db.commit();
    }

    for (int i = 0; i <= 5; i++) {
      db.begin();
      final Entity profile = db.newEntity("Profile");
      profile.setProperty("nick", "00" + i);
      profile.setProperty("name", "NickIndex" + i);
      profile.setProperty("surname", "NolteIndex" + i);
      db.save(profile);
      db.commit();
    }
  }

  @Test(dependsOnMethods = "testChangeOfIndexToUnique")
  public void removeNotUniqueIndexOnNick() {
    if (remoteDB) {
      return;
    }

    dropIndexes("Profile", "nick");
  }

  @Test(dependsOnMethods = "removeNotUniqueIndexOnNick")
  public void testQueryingWithoutNickIndex() {
    if (!remoteDB) {
      Assert.assertFalse(
          db.getMetadata().getSchema().getClassInternal("Profile")
              .getInvolvedIndexes(db, "name").isEmpty());

      Assert.assertTrue(
          db.getMetadata().getSchema().getClassInternal("Profile").getInvolvedIndexes(db, "nick")
              .isEmpty());
    }

    List<EntityImpl> result =
        db
            .command(new SQLSynchQuery<EntityImpl>("SELECT FROM Profile WHERE nick = 'Jay'"))
            .execute(db);
    Assert.assertEquals(result.size(), 2);

    result =
        db
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "SELECT FROM Profile WHERE nick = 'Jay' AND name = 'Jay'"))
            .execute(db);
    Assert.assertEquals(result.size(), 1);

    result =
        db
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "SELECT FROM Profile WHERE nick = 'Jay' AND name = 'Nick'"))
            .execute(db);
    Assert.assertEquals(result.size(), 1);
  }

  @Test(dependsOnMethods = "testQueryingWithoutNickIndex")
  public void createNotUniqueIndexOnNick() {
    db
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .createIndex(db, INDEX_TYPE.NOTUNIQUE);
  }

  @Test(dependsOnMethods = {"createNotUniqueIndexOnNick", "populateIndexDocuments"})
  public void testIndexInNotUniqueIndex() {
    if (!remoteDB) {
      Assert.assertEquals(
          db.getClassInternal("Profile").
              getInvolvedIndexesInternal(db, "nick").iterator()
              .next().getType(),
          SchemaClass.INDEX_TYPE.NOTUNIQUE.toString());
    }

    try (var resultSet =
        db.query(
            "SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0'"
                + " ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']")) {
      final List<String> expectedSurnames =
          new ArrayList<>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (final Entity profile : result) {
        expectedSurnames.remove(profile.<String>getProperty("surname"));
      }

      Assert.assertEquals(expectedSurnames.size(), 0);
    }
  }

  @Test
  public void indexLinks() {
    checkEmbeddedDB();

    db
        .getMetadata()
        .getSchema()
        .getClass("Whiz")
        .getProperty("account")
        .createIndex(db, INDEX_TYPE.NOTUNIQUE);

    final List<EntityImpl> result = executeQuery("select * from Account limit 1");
    final Index idx =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "Whiz.account");

    for (int i = 0; i < 5; i++) {
      db.begin();
      final EntityImpl whiz = ((EntityImpl) db.newEntity("Whiz"));

      whiz.field("id", i);
      whiz.field("text", "This is a test");
      whiz.field("account", result.get(0).getIdentity());

      whiz.save();
      db.commit();
    }

    Assert.assertEquals(idx.getInternal().size(db), 5);

    final List<EntityImpl> indexedResult =
        executeQuery("select * from Whiz where account = ?", result.get(0).getIdentity());
    Assert.assertEquals(indexedResult.size(), 5);

    db.begin();
    for (final EntityImpl resDoc : indexedResult) {
      db.bindToSession(resDoc).delete();
    }

    Entity whiz = db.newEntity("Whiz");
    whiz.setProperty("id", 100);
    whiz.setProperty("text", "This is a test!");
    whiz.setProperty("account", ((EntityImpl) db.newEntity("Company")).field("id", 9999));
    whiz.save();
    db.commit();

    db.begin();
    whiz = db.bindToSession(whiz);
    Assert.assertTrue(((EntityImpl) whiz.getProperty("account")).getIdentity().isValid());
    ((EntityImpl) whiz.getProperty("account")).delete();
    whiz.delete();
    db.commit();
  }

  public void linkedIndexedProperty() {
    try (DatabaseSessionInternal db = acquireSession()) {
      if (!db.getMetadata().getSchema().existsClass("TestClass")) {
        SchemaClass testClass =
            db.getMetadata().getSchema().createClass("TestClass", 1, (SchemaClass[]) null);
        SchemaClass testLinkClass =
            db.getMetadata().getSchema().createClass("TestLinkClass", 1, (SchemaClass[]) null);
        testClass
            .createProperty(db, "testLink", PropertyType.LINK, testLinkClass)
            .createIndex(db, INDEX_TYPE.NOTUNIQUE);
        testClass.createProperty(db, "name", PropertyType.STRING)
            .createIndex(db, INDEX_TYPE.UNIQUE);
        testLinkClass.createProperty(db, "testBoolean", PropertyType.BOOLEAN);
        testLinkClass.createProperty(db, "testString", PropertyType.STRING);
      }
      EntityImpl testClassDocument = db.newInstance("TestClass");
      db.begin();
      testClassDocument.field("name", "Test Class 1");
      EntityImpl testLinkClassDocument = ((EntityImpl) db.newEntity("TestLinkClass"));
      testLinkClassDocument.field("testString", "Test Link Class 1");
      testLinkClassDocument.field("testBoolean", true);
      testClassDocument.field("testLink", testLinkClassDocument);
      testClassDocument.save();
      db.commit();
      // THIS WILL THROW A java.lang.ClassCastException:
      // com.orientechnologies.core.id.RecordId cannot be cast to
      // java.lang.Boolean
      List<EntityImpl> result =
          db.query(
              new SQLSynchQuery<EntityImpl>(
                  "select from TestClass where testLink.testBoolean = true"));
      Assert.assertEquals(result.size(), 1);
      // THIS WILL THROW A java.lang.ClassCastException:
      // com.orientechnologies.core.id.RecordId cannot be cast to
      // java.lang.String
      result =
          db.query(
              new SQLSynchQuery<EntityImpl>(
                  "select from TestClass where testLink.testString = 'Test Link Class 1'"));
      Assert.assertEquals(result.size(), 1);
    }
  }

  @Test(dependsOnMethods = "linkedIndexedProperty")
  public void testLinkedIndexedPropertyInTx() {
    try (DatabaseSessionInternal db = acquireSession()) {
      db.begin();
      EntityImpl testClassDocument = db.newInstance("TestClass");
      testClassDocument.field("name", "Test Class 2");
      EntityImpl testLinkClassDocument = ((EntityImpl) db.newEntity("TestLinkClass"));
      testLinkClassDocument.field("testString", "Test Link Class 2");
      testLinkClassDocument.field("testBoolean", true);
      testClassDocument.field("testLink", testLinkClassDocument);
      testClassDocument.save();
      db.commit();

      // THIS WILL THROW A java.lang.ClassCastException:
      // com.orientechnologies.core.id.RecordId cannot be cast to
      // java.lang.Boolean
      List<EntityImpl> result =
          db.query(
              new SQLSynchQuery<EntityImpl>(
                  "select from TestClass where testLink.testBoolean = true"));
      Assert.assertEquals(result.size(), 2);
      // THIS WILL THROW A java.lang.ClassCastException:
      // com.orientechnologies.core.id.RecordId cannot be cast to
      // java.lang.String
      result =
          db.query(
              new SQLSynchQuery<EntityImpl>(
                  "select from TestClass where testLink.testString = 'Test Link Class 2'"));
      Assert.assertEquals(result.size(), 1);
    }
  }

  public void testConcurrentRemoveDelete() {
    checkEmbeddedDB();

    try (DatabaseSessionInternal db = acquireSession()) {
      if (!db.getMetadata().getSchema().existsClass("MyFruit")) {
        SchemaClass fruitClass = db.getMetadata().getSchema()
            .createClass("MyFruit", 1, (SchemaClass[]) null);
        fruitClass.createProperty(db, "name", PropertyType.STRING);
        fruitClass.createProperty(db, "color", PropertyType.STRING);

        db.getMetadata()
            .getSchema()
            .getClass("MyFruit")
            .getProperty("name")
            .createIndex(db, INDEX_TYPE.UNIQUE);

        db.getMetadata()
            .getSchema()
            .getClass("MyFruit")
            .getProperty("color")
            .createIndex(db, INDEX_TYPE.NOTUNIQUE);
      }

      long expectedIndexSize = 0;

      final int passCount = 10;
      final int chunkSize = 10;

      for (int pass = 0; pass < passCount; pass++) {
        List<EntityImpl> recordsToDelete = new ArrayList<>();
        db.begin();
        for (int i = 0; i < chunkSize; i++) {
          EntityImpl d =
              ((EntityImpl) db.newEntity("MyFruit"))
                  .field("name", "ABC" + pass + 'K' + i)
                  .field("color", "FOO" + pass);
          d.save();
          if (i < chunkSize / 2) {
            recordsToDelete.add(d);
          }
        }
        db.commit();

        expectedIndexSize += chunkSize;
        Assert.assertEquals(
            db.getMetadata()
                .getIndexManagerInternal()
                .getClassIndex(db, "MyFruit", "MyFruit.color")
                .getInternal()
                .size(db),
            expectedIndexSize,
            "After add");

        // do delete
        db.begin();
        for (final EntityImpl recordToDelete : recordsToDelete) {
          db.delete(db.bindToSession(recordToDelete));
        }
        db.commit();

        expectedIndexSize -= recordsToDelete.size();
        Assert.assertEquals(
            db.getMetadata()
                .getIndexManagerInternal()
                .getClassIndex(db, "MyFruit", "MyFruit.color")
                .getInternal()
                .size(db),
            expectedIndexSize,
            "After delete");
      }
    }
  }

  public void testIndexParamsAutoConversion() {
    checkEmbeddedDB();

    final EntityImpl doc;
    final RecordId result;
    try (DatabaseSessionInternal db = acquireSession()) {
      if (!db.getMetadata().getSchema().existsClass("IndexTestTerm")) {
        final SchemaClass termClass =
            db.getMetadata().getSchema().createClass("IndexTestTerm", 1, (SchemaClass[]) null);
        termClass.createProperty(db, "label", PropertyType.STRING);
        termClass.createIndex(db,
            "idxTerm",
            INDEX_TYPE.UNIQUE.toString(),
            null,
            Map.of("ignoreNullValues", true), new String[]{"label"});
      }

      db.begin();
      doc = ((EntityImpl) db.newEntity("IndexTestTerm"));
      doc.field("label", "42");
      doc.save();
      db.commit();

      try (Stream<RID> stream =
          db.getMetadata()
              .getIndexManagerInternal()
              .getIndex(db, "idxTerm")
              .getInternal()
              .getRids(db, "42")) {
        result = (RecordId) stream.findAny().orElse(null);
      }
    }
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getIdentity(), doc.getIdentity());
  }

  public void testTransactionUniqueIndexTestOne() {
    checkEmbeddedDB();

    DatabaseSessionInternal db = acquireSession();
    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexTest")) {
      final SchemaClass termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexTest", 1, (SchemaClass[]) null);
      termClass.createProperty(db, "label", PropertyType.STRING);
      termClass.createIndex(db,
          "idxTransactionUniqueIndexTest",
          INDEX_TYPE.UNIQUE.toString(),
          null,
          Map.of("ignoreNullValues", true), new String[]{"label"});
    }

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("TransactionUniqueIndexTest"));
    docOne.field("label", "A");
    docOne.save();
    db.commit();

    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "idxTransactionUniqueIndexTest");
    Assert.assertEquals(index.getInternal().size(this.db), 1);

    db.begin();
    try {
      EntityImpl docTwo = ((EntityImpl) db.newEntity("TransactionUniqueIndexTest"));
      docTwo.field("label", "A");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (RecordDuplicatedException ignored) {
    }

    Assert.assertEquals(index.getInternal().size(this.db), 1);
  }

  @Test(dependsOnMethods = "testTransactionUniqueIndexTestOne")
  public void testTransactionUniqueIndexTestTwo() {
    checkEmbeddedDB();

    DatabaseSessionInternal db = acquireSession();
    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexTest")) {
      final SchemaClass termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexTest", 1, (SchemaClass[]) null);

      termClass.createProperty(db, "label", PropertyType.STRING);
      termClass.createIndex(db,
          "idxTransactionUniqueIndexTest",
          INDEX_TYPE.UNIQUE.toString(),
          null,
          Map.of("ignoreNullValues", true), new String[]{"label"});
    }
    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "idxTransactionUniqueIndexTest");
    Assert.assertEquals(index.getInternal().size(this.db), 1);

    db.begin();
    try {
      EntityImpl docOne = ((EntityImpl) db.newEntity("TransactionUniqueIndexTest"));
      docOne.field("label", "B");
      docOne.save();

      EntityImpl docTwo = ((EntityImpl) db.newEntity("TransactionUniqueIndexTest"));
      docTwo.field("label", "B");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (RecordDuplicatedException oie) {
      db.rollback();
    }

    Assert.assertEquals(index.getInternal().size(this.db), 1);
  }

  public void testTransactionUniqueIndexTestWithDotNameOne() {
    checkEmbeddedDB();

    DatabaseSessionInternal db = acquireSession();
    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexWithDotTest")) {
      final SchemaClass termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexWithDotTest", 1, (SchemaClass[]) null);
      termClass.createProperty(db, "label", PropertyType.STRING).createIndex(db, INDEX_TYPE.UNIQUE);
    }

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("TransactionUniqueIndexWithDotTest"));
    docOne.field("label", "A");
    docOne.save();
    db.commit();

    final Index index =
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "TransactionUniqueIndexWithDotTest.label");
    Assert.assertEquals(index.getInternal().size(this.db), 1);

    long countClassBefore = db.countClass("TransactionUniqueIndexWithDotTest");
    db.begin();
    try {
      EntityImpl docTwo = ((EntityImpl) db.newEntity("TransactionUniqueIndexWithDotTest"));
      docTwo.field("label", "A");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (RecordDuplicatedException ignored) {
    }

    Assert.assertEquals(
        ((List<EntityImpl>)
            db.command(new CommandSQL("select from TransactionUniqueIndexWithDotTest"))
                .execute(db))
            .size(),
        countClassBefore);

    Assert.assertEquals(index.getInternal().size(db), 1);
  }

  @Test(dependsOnMethods = "testTransactionUniqueIndexTestWithDotNameOne")
  public void testTransactionUniqueIndexTestWithDotNameTwo() {
    checkEmbeddedDB();

    DatabaseSessionInternal db = acquireSession();
    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexWithDotTest")) {
      final SchemaClass termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexWithDotTest", 1, (SchemaClass[]) null);
      termClass.createProperty(db, "label", PropertyType.STRING)
          .createIndex(this.db, INDEX_TYPE.UNIQUE);
    }

    final Index index =
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "TransactionUniqueIndexWithDotTest.label");
    Assert.assertEquals(index.getInternal().size(this.db), 1);

    db.begin();
    try {
      EntityImpl docOne = ((EntityImpl) db.newEntity("TransactionUniqueIndexWithDotTest"));
      docOne.field("label", "B");
      docOne.save();

      EntityImpl docTwo = ((EntityImpl) db.newEntity("TransactionUniqueIndexWithDotTest"));
      docTwo.field("label", "B");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (RecordDuplicatedException oie) {
      db.rollback();
    }

    Assert.assertEquals(index.getInternal().size(this.db), 1);
  }

  @Test(dependsOnMethods = "linkedIndexedProperty")
  public void testIndexRemoval() {
    checkEmbeddedDB();

    final Index index = getIndex("Profile.nick");

    Iterator<RawPair<Object, RID>> streamIterator;
    Object key;
    try (Stream<RawPair<Object, RID>> stream = index.getInternal().stream(db)) {
      streamIterator = stream.iterator();
      Assert.assertTrue(streamIterator.hasNext());

      RawPair<Object, RID> pair = streamIterator.next();
      key = pair.first;

      db.begin();
      pair.second.getRecord(db).delete();
      db.commit();
    }

    try (Stream<RID> stream = index.getInternal().getRids(db, key)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  public void createInheritanceIndex() {
    try (DatabaseSessionInternal db = acquireSession()) {
      if (!db.getMetadata().getSchema().existsClass("BaseTestClass")) {
        SchemaClass baseClass =
            db.getMetadata().getSchema().createClass("BaseTestClass", 1, (SchemaClass[]) null);
        SchemaClass childClass =
            db.getMetadata().getSchema().createClass("ChildTestClass", 1, (SchemaClass[]) null);
        SchemaClass anotherChildClass =
            db.getMetadata().getSchema()
                .createClass("AnotherChildTestClass", 1, (SchemaClass[]) null);

        if (!baseClass.isSuperClassOf(childClass)) {
          childClass.setSuperClass(db, baseClass);
        }
        if (!baseClass.isSuperClassOf(anotherChildClass)) {
          anotherChildClass.setSuperClass(db, baseClass);
        }

        baseClass
            .createProperty(db, "testParentProperty", PropertyType.LONG)
            .createIndex(db, INDEX_TYPE.NOTUNIQUE);
      }

      db.begin();
      EntityImpl childClassDocument = db.newInstance("ChildTestClass");
      childClassDocument.field("testParentProperty", 10L);
      childClassDocument.save();

      EntityImpl anotherChildClassDocument = db.newInstance("AnotherChildTestClass");
      anotherChildClassDocument.field("testParentProperty", 11L);
      anotherChildClassDocument.save();
      db.commit();

      Assert.assertNotEquals(
          childClassDocument.getIdentity(), new RecordId(-1, RID.CLUSTER_POS_INVALID));
      Assert.assertNotEquals(
          anotherChildClassDocument.getIdentity(), new RecordId(-1, RID.CLUSTER_POS_INVALID));
    }
  }

  @Test(dependsOnMethods = "createInheritanceIndex")
  public void testIndexReturnOnlySpecifiedClass() {

    try (ResultSet result =
        db.command("select * from ChildTestClass where testParentProperty = 10")) {

      Assert.assertEquals(10L, result.next().<Object>getProperty("testParentProperty"));
      Assert.assertFalse(result.hasNext());
    }

    try (ResultSet result =
        db.command("select * from AnotherChildTestClass where testParentProperty = 11")) {
      Assert.assertEquals(11L, result.next().<Object>getProperty("testParentProperty"));
      Assert.assertFalse(result.hasNext());
    }
  }

  public void testNotUniqueIndexKeySize() {
    checkEmbeddedDB();

    final Schema schema = db.getMetadata().getSchema();
    SchemaClass cls = schema.createClass("IndexNotUniqueIndexKeySize");
    cls.createProperty(db, "value", PropertyType.INTEGER);
    cls.createIndex(db, "IndexNotUniqueIndexKeySizeIndex", INDEX_TYPE.NOTUNIQUE, "value");

    IndexManagerAbstract idxManager = db.getMetadata().getIndexManagerInternal();

    final Index idx = idxManager.getIndex(db, "IndexNotUniqueIndexKeySizeIndex");

    final Set<Integer> keys = new HashSet<>();
    for (int i = 1; i < 100; i++) {
      final Integer key = (int) Math.log(i);

      db.begin();
      final EntityImpl doc = ((EntityImpl) db.newEntity("IndexNotUniqueIndexKeySize"));
      doc.field("value", key);
      doc.save();
      db.commit();

      keys.add(key);
    }

    try (Stream<RawPair<Object, RID>> stream = idx.getInternal().stream(db)) {
      Assert.assertEquals(stream.map((pair) -> pair.first).distinct().count(), keys.size());
    }
  }

  public void testNotUniqueIndexSize() {
    checkEmbeddedDB();

    final Schema schema = db.getMetadata().getSchema();
    SchemaClass cls = schema.createClass("IndexNotUniqueIndexSize");
    cls.createProperty(db, "value", PropertyType.INTEGER);
    cls.createIndex(db, "IndexNotUniqueIndexSizeIndex", INDEX_TYPE.NOTUNIQUE, "value");

    IndexManagerAbstract idxManager = db.getMetadata().getIndexManagerInternal();
    final Index idx = idxManager.getIndex(db, "IndexNotUniqueIndexSizeIndex");

    for (int i = 1; i < 100; i++) {
      final Integer key = (int) Math.log(i);

      db.begin();
      final EntityImpl doc = ((EntityImpl) db.newEntity("IndexNotUniqueIndexSize"));
      doc.field("value", key);
      doc.save();
      db.commit();
    }

    Assert.assertEquals(idx.getInternal().size(db), 99);
  }

  @Test
  public void testIndexRebuildDuringNonProxiedObjectDelete() {
    checkEmbeddedDB();

    db.begin();
    Entity profile = db.newEntity("Profile");
    profile.setProperty("nick", "NonProxiedObjectToDelete");
    profile.setProperty("name", "NonProxiedObjectToDelete");
    profile.setProperty("surname", "NonProxiedObjectToDelete");
    profile = db.save(profile);
    db.commit();

    IndexManagerAbstract idxManager = db.getMetadata().getIndexManagerInternal();
    Index nickIndex = idxManager.getIndex(db, "Profile.nick");

    try (Stream<RID> stream = nickIndex.getInternal()
        .getRids(db, "NonProxiedObjectToDelete")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    db.begin();
    final Entity loadedProfile = db.load(profile.getIdentity());
    db.delete(loadedProfile);
    db.commit();

    try (Stream<RID> stream = nickIndex.getInternal()
        .getRids(db, "NonProxiedObjectToDelete")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test(dependsOnMethods = "testIndexRebuildDuringNonProxiedObjectDelete")
  public void testIndexRebuildDuringDetachAllNonProxiedObjectDelete() {
    checkEmbeddedDB();

    db.begin();
    Entity profile = db.newEntity("Profile");
    profile.setProperty("nick", "NonProxiedObjectToDelete");
    profile.setProperty("name", "NonProxiedObjectToDelete");
    profile.setProperty("surname", "NonProxiedObjectToDelete");
    profile = db.save(profile);
    db.commit();

    IndexManagerAbstract idxManager = db.getMetadata().getIndexManagerInternal();
    Index nickIndex = idxManager.getIndex(db, "Profile.nick");

    try (Stream<RID> stream = nickIndex.getInternal()
        .getRids(db, "NonProxiedObjectToDelete")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    final Entity loadedProfile = db.load(profile.getIdentity());
    db.begin();
    db.delete(db.bindToSession(loadedProfile));
    db.commit();

    try (Stream<RID> stream = nickIndex.getInternal()
        .getRids(db, "NonProxiedObjectToDelete")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test(dependsOnMethods = "testIndexRebuildDuringDetachAllNonProxiedObjectDelete")
  public void testRestoreUniqueIndex() {
    dropIndexes("Profile", "nick");
    db
        .command(
            "CREATE INDEX Profile.nick on Profile (nick) UNIQUE METADATA {ignoreNullValues: true}")
        .close();
    db.getMetadata().reload();
  }

  @Test
  public void testIndexInCompositeQuery() {
    SchemaClass classOne =
        db.getMetadata().getSchema()
            .createClass("CompoundSQLIndexTest1", 1, (SchemaClass[]) null);
    SchemaClass classTwo =
        db.getMetadata().getSchema()
            .createClass("CompoundSQLIndexTest2", 1, (SchemaClass[]) null);

    classTwo.createProperty(db, "address", PropertyType.LINK, classOne);

    classTwo.createIndex(db, "CompoundSQLIndexTestIndex", INDEX_TYPE.UNIQUE, "address");

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("CompoundSQLIndexTest1"));
    docOne.field("city", "Montreal");

    docOne.save();

    EntityImpl docTwo = ((EntityImpl) db.newEntity("CompoundSQLIndexTest2"));
    docTwo.field("address", docOne);
    docTwo.save();
    db.commit();

    List<EntityImpl> result =
        executeQuery(
            "select from CompoundSQLIndexTest2 where address in (select from"
                + " CompoundSQLIndexTest1 where city='Montreal')");
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).getIdentity(), docTwo.getIdentity());
  }

  public void testIndexWithLimitAndOffset() {
    final Schema schema = db.getSchema();
    final SchemaClass indexWithLimitAndOffset =
        schema.createClass("IndexWithLimitAndOffsetClass", 1, (SchemaClass[]) null);
    indexWithLimitAndOffset.createProperty(db, "val", PropertyType.INTEGER);
    indexWithLimitAndOffset.createProperty(db, "index", PropertyType.INTEGER);

    db
        .command(
            "create index IndexWithLimitAndOffset on IndexWithLimitAndOffsetClass (val) notunique")
        .close();

    for (int i = 0; i < 30; i++) {
      db.begin();
      final EntityImpl document = ((EntityImpl) db.newEntity("IndexWithLimitAndOffsetClass"));
      document.field("val", i / 10);
      document.field("index", i);
      document.save();
      db.commit();
    }

    final List<EntityImpl> result =
        executeQuery("select from IndexWithLimitAndOffsetClass where val = 1 offset 5 limit 2");
    Assert.assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final EntityImpl document = result.get(i);
      Assert.assertEquals(document.<Object>field("val"), 1);
      Assert.assertEquals(document.<Object>field("index"), 15 + i);
    }
  }

  public void testNullIndexKeysSupport() {
    final Schema schema = db.getSchema();
    final SchemaClass clazz = schema.createClass("NullIndexKeysSupport", 1, (SchemaClass[]) null);
    clazz.createProperty(db, "nullField", PropertyType.STRING);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "NullIndexKeysSupportIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"nullField"});
    for (int i = 0; i < 20; i++) {
      db.begin();
      if (i % 5 == 0) {
        EntityImpl document = ((EntityImpl) db.newEntity("NullIndexKeysSupport"));
        document.field("nullField", (Object) null);
        document.save();
      } else {
        EntityImpl document = ((EntityImpl) db.newEntity("NullIndexKeysSupport"));
        document.field("nullField", "val" + i);
        document.save();
      }
      db.commit();
    }

    List<EntityImpl> result =
        executeQuery("select from NullIndexKeysSupport where nullField = 'val3'");
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).field("nullField"), "val3");

    final String query = "select from NullIndexKeysSupport where nullField is null";
    result = executeQuery("select from NullIndexKeysSupport where nullField is null");

    Assert.assertEquals(result.size(), 4);
    for (EntityImpl document : result) {
      Assert.assertNull(document.field("nullField"));
    }

    final EntityImpl explain = db.command(new CommandSQL("explain " + query))
        .execute(db);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("NullIndexKeysSupportIndex"));
  }

  public void testNullHashIndexKeysSupport() {
    final Schema schema = db.getSchema();
    final SchemaClass clazz = schema.createClass("NullHashIndexKeysSupport", 1,
        (SchemaClass[]) null);
    clazz.createProperty(db, "nullField", PropertyType.STRING);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "NullHashIndexKeysSupportIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"nullField"});
    for (int i = 0; i < 20; i++) {
      db.begin();
      if (i % 5 == 0) {
        EntityImpl document = ((EntityImpl) db.newEntity("NullHashIndexKeysSupport"));
        document.field("nullField", (Object) null);
        document.save();
      } else {
        EntityImpl document = ((EntityImpl) db.newEntity("NullHashIndexKeysSupport"));
        document.field("nullField", "val" + i);
        document.save();
      }
      db.commit();
    }

    List<EntityImpl> result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullHashIndexKeysSupport where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).field("nullField"), "val3");

    final String query = "select from NullHashIndexKeysSupport where nullField is null";
    result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullHashIndexKeysSupport where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (EntityImpl document : result) {
      Assert.assertNull(document.field("nullField"));
    }

    final EntityImpl explain = db.command(new CommandSQL("explain " + query))
        .execute(db);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("NullHashIndexKeysSupportIndex"));
  }

  public void testNullIndexKeysSupportInTx() {
    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass clazz = schema.createClass("NullIndexKeysSupportInTx", 1,
        (SchemaClass[]) null);
    clazz.createProperty(db, "nullField", PropertyType.STRING);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "NullIndexKeysSupportInTxIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"nullField"});

    db.begin();

    for (int i = 0; i < 20; i++) {
      if (i % 5 == 0) {
        EntityImpl document = ((EntityImpl) db.newEntity("NullIndexKeysSupportInTx"));
        document.field("nullField", (Object) null);
        document.save();
      } else {
        EntityImpl document = ((EntityImpl) db.newEntity("NullIndexKeysSupportInTx"));
        document.field("nullField", "val" + i);
        document.save();
      }
    }

    db.commit();

    List<EntityImpl> result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullIndexKeysSupportInTx where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).field("nullField"), "val3");

    final String query = "select from NullIndexKeysSupportInTx where nullField is null";
    result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullIndexKeysSupportInTx where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (EntityImpl document : result) {
      Assert.assertNull(document.field("nullField"));
    }

    final EntityImpl explain = db.command(new CommandSQL("explain " + query))
        .execute(db);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("NullIndexKeysSupportInTxIndex"));
  }

  public void testNullIndexKeysSupportInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final Schema schema = db.getSchema();
    final SchemaClass clazz = schema.createClass("NullIndexKeysSupportInMiddleTx", 1,
        (SchemaClass[]) null);
    clazz.createProperty(db, "nullField", PropertyType.STRING);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "NullIndexKeysSupportInMiddleTxIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"nullField"});

    db.begin();

    for (int i = 0; i < 20; i++) {
      if (i % 5 == 0) {
        EntityImpl document = ((EntityImpl) db.newEntity("NullIndexKeysSupportInMiddleTx"));
        document.field("nullField", (Object) null);
        document.save();
      } else {
        EntityImpl document = ((EntityImpl) db.newEntity("NullIndexKeysSupportInMiddleTx"));
        document.field("nullField", "val" + i);
        document.save();
      }
    }

    List<EntityImpl> result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullIndexKeysSupportInMiddleTx where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).field("nullField"), "val3");

    final String query = "select from NullIndexKeysSupportInMiddleTx where nullField is null";
    result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullIndexKeysSupportInMiddleTx where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (EntityImpl document : result) {
      Assert.assertNull(document.field("nullField"));
    }

    final EntityImpl explain = db.command(new CommandSQL("explain " + query))
        .execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("NullIndexKeysSupportInMiddleTxIndex"));

    db.commit();
  }

  public void testCreateIndexAbstractClass() {
    final Schema schema = db.getSchema();

    SchemaClass abstractClass = schema.createAbstractClass("TestCreateIndexAbstractClass");
    abstractClass
        .createProperty(db, "value", PropertyType.STRING)
        .setMandatory(db, true)
        .createIndex(db, INDEX_TYPE.UNIQUE);

    schema.createClass("TestCreateIndexAbstractClassChildOne", abstractClass);
    schema.createClass("TestCreateIndexAbstractClassChildTwo", abstractClass);

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("TestCreateIndexAbstractClassChildOne"));
    docOne.field("value", "val1");
    docOne.save();

    EntityImpl docTwo = ((EntityImpl) db.newEntity("TestCreateIndexAbstractClassChildTwo"));
    docTwo.field("value", "val2");
    docTwo.save();
    db.commit();

    final String queryOne = "select from TestCreateIndexAbstractClass where value = 'val1'";

    List<EntityImpl> resultOne = executeQuery(queryOne);
    Assert.assertEquals(resultOne.size(), 1);
    Assert.assertEquals(resultOne.get(0).getIdentity(), docOne.getIdentity());

    try (var result = db.command("explain " + queryOne)) {
      var explain = result.next().toEntity();
      Assert.assertTrue(
          explain
              .<String>getProperty("executionPlanAsString")
              .contains("FETCH FROM INDEX TestCreateIndexAbstractClass.value"));

      final String queryTwo = "select from TestCreateIndexAbstractClass where value = 'val2'";

      List<EntityImpl> resultTwo = executeQuery(queryTwo);
      Assert.assertEquals(resultTwo.size(), 1);
      Assert.assertEquals(resultTwo.get(0).getIdentity(), docTwo.getIdentity());

      explain = db.command(new CommandSQL("explain " + queryTwo)).execute(db);
      Assert.assertTrue(
          explain
              .<Collection<String>>getProperty("involvedIndexes")
              .contains("TestCreateIndexAbstractClass.value"));
    }
  }

  @Test(enabled = false)
  public void testValuesContainerIsRemovedIfIndexIsRemoved() {
    if (remoteDB) {
      return;
    }

    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz =
        schema.createClass("ValuesContainerIsRemovedIfIndexIsRemovedClass", 1,
            (SchemaClass[]) null);
    clazz.createProperty(db, "val", PropertyType.STRING);

    db
        .command(
            "create index ValuesContainerIsRemovedIfIndexIsRemovedIndex on"
                + " ValuesContainerIsRemovedIfIndexIsRemovedClass (val) notunique")
        .close();

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 100; j++) {
        db.begin();
        EntityImpl document = ((EntityImpl) db.newEntity(
            "ValuesContainerIsRemovedIfIndexIsRemovedClass"));
        document.field("val", "value" + i);
        document.save();
        db.commit();
      }
    }

    final AbstractPaginatedStorage storageLocalAbstract =
        (AbstractPaginatedStorage)
            ((DatabaseSessionInternal) db.getUnderlying()).getStorage();

    final WriteCache writeCache = storageLocalAbstract.getWriteCache();
    Assert.assertTrue(writeCache.exists("ValuesContainerIsRemovedIfIndexIsRemovedIndex.irs"));
    db.command("drop index ValuesContainerIsRemovedIfIndexIsRemovedIndex").close();
    Assert.assertFalse(writeCache.exists("ValuesContainerIsRemovedIfIndexIsRemovedIndex.irs"));
  }

  public void testPreservingIdentityInIndexTx() {
    checkEmbeddedDB();
    if (!db.getMetadata().getSchema().existsClass("PreservingIdentityInIndexTxParent")) {
      db.createVertexClass("PreservingIdentityInIndexTxParent");
    }
    if (!db.getMetadata().getSchema().existsClass("PreservingIdentityInIndexTxEdge")) {
      db.createEdgeClass("PreservingIdentityInIndexTxEdge");
    }
    var fieldClass = (SchemaClassInternal) db.getClass("PreservingIdentityInIndexTxChild");
    if (fieldClass == null) {
      fieldClass = (SchemaClassInternal) db.createVertexClass(
          "PreservingIdentityInIndexTxChild");
      fieldClass.createProperty(db, "name", PropertyType.STRING);
      fieldClass.createProperty(db, "in_field", PropertyType.LINK);
      fieldClass.createIndex(db, "nameParentIndex", INDEX_TYPE.NOTUNIQUE, "in_field", "name");
    }

    db.begin();
    Vertex parent = db.newVertex("PreservingIdentityInIndexTxParent");
    db.save(parent);
    Vertex child = db.newVertex("PreservingIdentityInIndexTxChild");
    db.save(child);
    db.save(db.newRegularEdge(parent, child, "PreservingIdentityInIndexTxEdge"));
    child.setProperty("name", "pokus");
    db.save(child);

    Vertex parent2 = db.newVertex("PreservingIdentityInIndexTxParent");
    db.save(parent2);
    Vertex child2 = db.newVertex("PreservingIdentityInIndexTxChild");
    db.save(child2);
    db.save(db.newRegularEdge(parent2, child2, "preservingIdentityInIndexTxEdge"));
    child2.setProperty("name", "pokus2");
    db.save(child2);
    db.commit();

    {
      fieldClass = db.getClassInternal("PreservingIdentityInIndexTxChild");
      Index index = fieldClass.getClassIndex(db, "nameParentIndex");
      CompositeKey key = new CompositeKey(parent.getIdentity(), "pokus");

      Collection<RID> h;
      try (Stream<RID> stream = index.getInternal().getRids(db, key)) {
        h = stream.toList();
      }
      for (RID o : h) {
        Assert.assertNotNull(db.load(o));
      }
    }

    {
      fieldClass = (SchemaClassInternal) db.getClass("PreservingIdentityInIndexTxChild");
      Index index = fieldClass.getClassIndex(db, "nameParentIndex");
      CompositeKey key = new CompositeKey(parent2.getIdentity(), "pokus2");

      Collection<RID> h;
      try (Stream<RID> stream = index.getInternal().getRids(db, key)) {
        h = stream.toList();
      }
      for (RID o : h) {
        Assert.assertNotNull(db.load(o));
      }
    }

    db.begin();
    db.delete(db.bindToSession(parent));
    db.delete(db.bindToSession(child));

    db.delete(db.bindToSession(parent2));
    db.delete(db.bindToSession(child2));
    db.commit();
  }

  public void testEmptyNotUniqueIndex() {
    checkEmbeddedDB();

    SchemaClass emptyNotUniqueIndexClazz =
        db
            .getMetadata()
            .getSchema()
            .createClass("EmptyNotUniqueIndexTest", 1, (SchemaClass[]) null);
    emptyNotUniqueIndexClazz.createProperty(db, "prop", PropertyType.STRING);

    emptyNotUniqueIndexClazz.createIndex(db,
        "EmptyNotUniqueIndexTestIndex", INDEX_TYPE.NOTUNIQUE, "prop");
    final Index notUniqueIndex = db.getIndex("EmptyNotUniqueIndexTestIndex");

    db.begin();
    EntityImpl document = ((EntityImpl) db.newEntity("EmptyNotUniqueIndexTest"));
    document.field("prop", "keyOne");
    document.save();

    document = ((EntityImpl) db.newEntity("EmptyNotUniqueIndexTest"));
    document.field("prop", "keyTwo");
    document.save();
    db.commit();

    try (Stream<RID> stream = notUniqueIndex.getInternal().getRids(db, "RandomKeyOne")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = notUniqueIndex.getInternal().getRids(db, "keyOne")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    try (Stream<RID> stream = notUniqueIndex.getInternal().getRids(db, "RandomKeyTwo")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = notUniqueIndex.getInternal().getRids(db, "keyTwo")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  public void testNullIteration() {
    SchemaClass v = db.getSchema().getClass("V");
    SchemaClass testNullIteration =
        db.getMetadata().getSchema().createClass("NullIterationTest", v);
    testNullIteration.createProperty(db, "name", PropertyType.STRING);
    testNullIteration.createProperty(db, "birth", PropertyType.DATETIME);

    db.begin();
    db
        .command("CREATE VERTEX NullIterationTest SET name = 'Andrew', birth = sysdate()")
        .close();
    db
        .command("CREATE VERTEX NullIterationTest SET name = 'Marcel', birth = sysdate()")
        .close();
    db.command("CREATE VERTEX NullIterationTest SET name = 'Olivier'").close();
    db.commit();

    var metadata = Map.of("ignoreNullValues", false);

    testNullIteration.createIndex(db,
        "NullIterationTestIndex",
        INDEX_TYPE.NOTUNIQUE.name(),
        null,
        metadata, new String[]{"birth"});

    ResultSet result = db.query("SELECT FROM NullIterationTest ORDER BY birth ASC");
    Assert.assertEquals(result.stream().count(), 3);

    result = db.query("SELECT FROM NullIterationTest ORDER BY birth DESC");
    Assert.assertEquals(result.stream().count(), 3);

    result = db.query("SELECT FROM NullIterationTest");
    Assert.assertEquals(result.stream().count(), 3);
  }

  public void testMultikeyWithoutFieldAndNullSupport() {
    checkEmbeddedDB();

    // generates stubs for index
    db.begin();
    EntityImpl doc1 = ((EntityImpl) db.newEntity());
    doc1.save();
    EntityImpl doc2 = ((EntityImpl) db.newEntity());
    doc2.save();
    EntityImpl doc3 = ((EntityImpl) db.newEntity());
    doc3.save();
    EntityImpl doc4 = ((EntityImpl) db.newEntity());
    doc4.save();
    db.commit();

    final RID rid1 = doc1.getIdentity();
    final RID rid2 = doc2.getIdentity();
    final RID rid3 = doc3.getIdentity();
    final RID rid4 = doc4.getIdentity();
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("TestMultikeyWithoutField");

    clazz.createProperty(db, "state", PropertyType.BYTE);
    clazz.createProperty(db, "users", PropertyType.LINKSET);
    clazz.createProperty(db, "time", PropertyType.LONG);
    clazz.createProperty(db, "reg", PropertyType.LONG);
    clazz.createProperty(db, "no", PropertyType.INTEGER);

    var mt = Map.of("ignoreNullValues", false);
    clazz.createIndex(db,
        "MultikeyWithoutFieldIndex",
        INDEX_TYPE.UNIQUE.toString(),
        null,
        mt, new String[]{"state", "users", "time", "reg", "no"});

    db.begin();
    EntityImpl document = ((EntityImpl) db.newEntity("TestMultikeyWithoutField"));
    document.field("state", (byte) 1);

    Set<RID> users = new HashSet<>();
    users.add(rid1);
    users.add(rid2);

    document.field("users", users);
    document.field("time", 12L);
    document.field("reg", 14L);
    document.field("no", 12);

    document.save();
    db.commit();

    Index index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    // we support first and last keys check only for embedded storage
    // we support first and last keys check only for embedded storage
    if (!(db.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        if (rid1.compareTo(rid2) < 0) {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid1, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
        }
      }
      try (Stream<RawPair<Object, RID>> descStream = index.getInternal().descStream(db)) {
        if (rid1.compareTo(rid2) < 0) {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid1, 12L, 14L, 12));
        }
      }
    }

    final RID rid = document.getIdentity();

    db.close();
    db = acquireSession();

    db.begin();
    document = db.load(rid);

    users = document.field("users");
    users.remove(rid1);
    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(db), 1);
    if (!(db.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
      }
    }

    db.close();
    db = acquireSession();

    document = db.load(rid);

    db.begin();
    document = db.bindToSession(document);
    users = document.field("users");
    users.remove(rid2);
    Assert.assertTrue(users.isEmpty());
    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndex");

    Assert.assertEquals(index.getInternal().size(db), 1);
    if (!(db.isRemote())) {
      try (Stream<Object> keyStreamAsc = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStreamAsc.iterator().next(), new CompositeKey((byte) 1, null, 12L, 14L, 12));
      }
    }

    db.close();
    db = acquireSession();

    db.begin();
    document = db.load(rid);
    users = document.field("users");
    users.add(rid3);
    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndex");

    Assert.assertEquals(index.getInternal().size(db), 1);
    if (!(db.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
      }
    }

    db.close();
    db = acquireSession();

    db.begin();
    document = db.bindToSession(document);
    users = document.field("users");
    users.add(rid4);
    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    if (!(db.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        if (rid3.compareTo(rid4) < 0) {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid4, 12L, 14L, 12));
        }
      }
      try (Stream<RawPair<Object, RID>> descStream = index.getInternal().descStream(db)) {
        if (rid3.compareTo(rid4) < 0) {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid4, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
        }
      }
    }

    db.close();
    db = acquireSession();

    db.begin();
    document = db.bindToSession(document);
    document.removeField("users");
    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(db), 1);

    if (!(db.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, null, 12L, 14L, 12));
      }
    }
  }

  public void testMultikeyWithoutFieldAndNoNullSupport() {
    checkEmbeddedDB();

    // generates stubs for index
    db.begin();
    EntityImpl doc1 = ((EntityImpl) db.newEntity());
    doc1.save();
    EntityImpl doc2 = ((EntityImpl) db.newEntity());
    doc2.save();
    EntityImpl doc3 = ((EntityImpl) db.newEntity());
    doc3.save();
    EntityImpl doc4 = ((EntityImpl) db.newEntity());
    doc4.save();
    db.commit();

    final RID rid1 = doc1.getIdentity();
    final RID rid2 = doc2.getIdentity();
    final RID rid3 = doc3.getIdentity();
    final RID rid4 = doc4.getIdentity();

    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("TestMultikeyWithoutFieldNoNullSupport");

    clazz.createProperty(db, "state", PropertyType.BYTE);
    clazz.createProperty(db, "users", PropertyType.LINKSET);
    clazz.createProperty(db, "time", PropertyType.LONG);
    clazz.createProperty(db, "reg", PropertyType.LONG);
    clazz.createProperty(db, "no", PropertyType.INTEGER);

    clazz.createIndex(db,
        "MultikeyWithoutFieldIndexNoNullSupport",
        INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true),
        new String[]{"state", "users", "time", "reg", "no"});

    EntityImpl document = ((EntityImpl) db.newEntity("TestMultikeyWithoutFieldNoNullSupport"));
    document.field("state", (byte) 1);

    Set<RID> users = new HashSet<>();
    users.add(rid1);
    users.add(rid2);

    document.field("users", users);
    document.field("time", 12L);
    document.field("reg", 14L);
    document.field("no", 12);

    db.begin();
    document.save();
    db.commit();

    Index index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(db), 2);

    // we support first and last keys check only for embedded storage
    if (!(db.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        if (rid1.compareTo(rid2) < 0) {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid1, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
        }
      }
      try (Stream<RawPair<Object, RID>> descStream = index.getInternal().descStream(db)) {
        if (rid1.compareTo(rid2) < 0) {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid1, 12L, 14L, 12));
        }
      }
    }

    final RID rid = document.getIdentity();

    db.close();
    db = acquireSession();

    db.begin();
    document = db.load(rid);
    users = document.field("users");
    users.remove(rid1);

    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(db), 1);
    if (!(db.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
      }
    }

    db.close();
    db = acquireSession();

    db.begin();
    document = db.load(rid);

    users = document.field("users");
    users.remove(rid2);
    Assert.assertTrue(users.isEmpty());

    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(db), 0);

    db.close();
    db = acquireSession();
    db.begin();

    document = db.load(rid);
    users = document.field("users");
    users.add(rid3);

    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(db), 1);

    if (!(db.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
      }
    }

    db.close();
    db = acquireSession();

    db.begin();

    document = db.bindToSession(document);
    users = document.field("users");
    users.add(rid4);

    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(db), 2);

    if (!(db.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        if (rid3.compareTo(rid4) < 0) {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid4, 12L, 14L, 12));
        }
      }
      try (Stream<RawPair<Object, RID>> descStream = index.getInternal().descStream(db)) {
        if (rid3.compareTo(rid4) < 0) {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid4, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
        }
      }
    }

    db.close();
    db = acquireSession();

    db.begin();
    document = db.bindToSession(document);
    document.removeField("users");

    document.save();
    db.commit();

    index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testNullValuesCountSBTreeUnique() {
    checkEmbeddedDB();

    SchemaClass nullSBTreeClass = db.getSchema().createClass("NullValuesCountSBTreeUnique");
    nullSBTreeClass.createProperty(db, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(db, "NullValuesCountSBTreeUniqueIndex", INDEX_TYPE.UNIQUE,
        "field");

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("NullValuesCountSBTreeUnique"));
    docOne.field("field", 1);
    docOne.save();

    EntityImpl docTwo = ((EntityImpl) db.newEntity("NullValuesCountSBTreeUnique"));
    docTwo.field("field", (Integer) null);
    docTwo.save();
    db.commit();

    Index index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountSBTreeUniqueIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);
    try (Stream<RawPair<Object, RID>> stream = index.getInternal().stream(db)) {
      try (Stream<RID> nullStream = index.getInternal().getRids(db, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountSBTreeNotUniqueOne() {
    checkEmbeddedDB();

    SchemaClass nullSBTreeClass =
        db.getMetadata().getSchema().createClass("NullValuesCountSBTreeNotUniqueOne");
    nullSBTreeClass.createProperty(db, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(db,
        "NullValuesCountSBTreeNotUniqueOneIndex", INDEX_TYPE.NOTUNIQUE, "field");

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("NullValuesCountSBTreeNotUniqueOne"));
    docOne.field("field", 1);
    docOne.save();

    EntityImpl docTwo = ((EntityImpl) db.newEntity("NullValuesCountSBTreeNotUniqueOne"));
    docTwo.field("field", (Integer) null);
    docTwo.save();
    db.commit();

    Index index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountSBTreeNotUniqueOneIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);
    try (Stream<RawPair<Object, RID>> stream = index.getInternal().stream(db)) {
      try (Stream<RID> nullStream = index.getInternal().getRids(db, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountSBTreeNotUniqueTwo() {
    checkEmbeddedDB();

    SchemaClass nullSBTreeClass =
        db.getMetadata().getSchema().createClass("NullValuesCountSBTreeNotUniqueTwo");
    nullSBTreeClass.createProperty(db, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(db,
        "NullValuesCountSBTreeNotUniqueTwoIndex", INDEX_TYPE.NOTUNIQUE, "field");

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("NullValuesCountSBTreeNotUniqueTwo"));
    docOne.field("field", (Integer) null);
    docOne.save();

    EntityImpl docTwo = ((EntityImpl) db.newEntity("NullValuesCountSBTreeNotUniqueTwo"));
    docTwo.field("field", (Integer) null);
    docTwo.save();
    db.commit();

    Index index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountSBTreeNotUniqueTwoIndex");
    try (Stream<RawPair<Object, RID>> stream = index.getInternal().stream(db)) {
      try (Stream<RID> nullStream = index.getInternal().getRids(db, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count()
                + nullStream.findAny().map(v -> 1).orElse(0),
            1);
      }
    }
    Assert.assertEquals(index.getInternal().size(db), 2);
  }

  public void testNullValuesCountHashUnique() {
    checkEmbeddedDB();
    SchemaClass nullSBTreeClass = db.getSchema().createClass("NullValuesCountHashUnique");
    nullSBTreeClass.createProperty(db, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(db,
        "NullValuesCountHashUniqueIndex", INDEX_TYPE.UNIQUE, "field");

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("NullValuesCountHashUnique"));
    docOne.field("field", 1);
    docOne.save();

    EntityImpl docTwo = ((EntityImpl) db.newEntity("NullValuesCountHashUnique"));
    docTwo.field("field", (Integer) null);
    docTwo.save();
    db.commit();

    Index index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountHashUniqueIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);
    try (Stream<RawPair<Object, RID>> stream = index.getInternal().stream(db)) {
      try (Stream<RID> nullStream = index.getInternal().getRids(db, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountHashNotUniqueOne() {
    checkEmbeddedDB();

    SchemaClass nullSBTreeClass = db.getSchema()
        .createClass("NullValuesCountHashNotUniqueOne");
    nullSBTreeClass.createProperty(db, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(db,
        "NullValuesCountHashNotUniqueOneIndex", INDEX_TYPE.NOTUNIQUE, "field");

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("NullValuesCountHashNotUniqueOne"));
    docOne.field("field", 1);
    docOne.save();

    EntityImpl docTwo = ((EntityImpl) db.newEntity("NullValuesCountHashNotUniqueOne"));
    docTwo.field("field", (Integer) null);
    docTwo.save();
    db.commit();

    Index index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountHashNotUniqueOneIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);
    try (Stream<RawPair<Object, RID>> stream = index.getInternal().stream(db)) {
      try (Stream<RID> nullStream = index.getInternal().getRids(db, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountHashNotUniqueTwo() {
    checkEmbeddedDB();

    SchemaClass nullSBTreeClass =
        db.getMetadata().getSchema().createClass("NullValuesCountHashNotUniqueTwo");
    nullSBTreeClass.createProperty(db, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(db,
        "NullValuesCountHashNotUniqueTwoIndex", INDEX_TYPE.NOTUNIQUE, "field");

    db.begin();
    EntityImpl docOne = ((EntityImpl) db.newEntity("NullValuesCountHashNotUniqueTwo"));
    docOne.field("field", (Integer) null);
    docOne.save();

    EntityImpl docTwo = ((EntityImpl) db.newEntity("NullValuesCountHashNotUniqueTwo"));
    docTwo.field("field", (Integer) null);
    docTwo.save();
    db.commit();

    Index index =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountHashNotUniqueTwoIndex");
    try (Stream<RawPair<Object, RID>> stream = index.getInternal().stream(db)) {
      try (Stream<RID> nullStream = index.getInternal().getRids(db, null)) {
        Assert.assertEquals(
            stream.map(pair -> pair.first).distinct().count()
                + nullStream.findAny().map(v -> 1).orElse(0),
            1);
      }
    }
    Assert.assertEquals(index.getInternal().size(db), 2);
  }

  @Test
  public void testParamsOrder() {
    db.command("CREATE CLASS Task extends V").close();
    db
        .command("CREATE PROPERTY Task.projectId STRING (MANDATORY TRUE, NOTNULL, MAX 20)")
        .close();
    db.command("CREATE PROPERTY Task.seq SHORT ( MANDATORY TRUE, NOTNULL, MIN 0)").close();
    db.command("CREATE INDEX TaskPK ON Task (projectId, seq) UNIQUE").close();

    db.begin();
    db.command("INSERT INTO Task (projectId, seq) values ( 'foo', 2)").close();
    db.command("INSERT INTO Task (projectId, seq) values ( 'bar', 3)").close();
    db.commit();

    var results =
        db
            .query("select from Task where projectId = 'foo' and seq = 2")
            .vertexStream()
            .toList();
    Assert.assertEquals(results.size(), 1);
  }

  private void assertIndexUsage(ResultSet resultSet) {
    var executionPlan = resultSet.getExecutionPlan().orElseThrow();
    for (var step : executionPlan.getSteps()) {
      if (assertIndexUsage(step, "Profile.nick")) {
        return;
      }
    }

    Assert.fail("Index " + "Profile.nick" + " was not used in the query");
  }

  private boolean assertIndexUsage(ExecutionStep executionStep, String indexName) {
    if (executionStep instanceof FetchFromIndexStep fetchFromIndexStep
        && fetchFromIndexStep.getIndexName().equals(indexName)) {
      return true;
    }
    for (var subStep : executionStep.getSubSteps()) {
      if (assertIndexUsage(subStep, indexName)) {
        return true;
      }
    }

    return false;
  }
}
