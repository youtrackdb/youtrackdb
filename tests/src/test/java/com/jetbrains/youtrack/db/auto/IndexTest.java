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
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.executor.FetchFromIndexStep;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    var jayMiner = session.newEntity("Profile");
    jayMiner.setProperty("nick", "Jay");
    jayMiner.setProperty("name", "Jay");
    jayMiner.setProperty("surname", "Miner");

    session.begin();
    session.commit();

    var jacobMiner = session.newEntity("Profile");
    jacobMiner.setProperty("nick", "Jay");
    jacobMiner.setProperty("name", "Jacob");
    jacobMiner.setProperty("surname", "Miner");

    try {
      session.begin();
      session.commit();

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
        session.getMetadata().getSchema().getClassInternal("Profile")
            .getInvolvedIndexesInternal(session, "nick").iterator().next().getType(),
        SchemaClass.INDEX_TYPE.UNIQUE.toString());
    try (var resultSet =
        session.query(
            "SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0'"
                + " ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']")) {
      assertIndexUsage(resultSet);

      final List<String> expectedSurnames =
          new ArrayList<>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (final var profile : result) {
        expectedSurnames.remove(profile.<String>getProperty("surname"));
      }

      Assert.assertEquals(expectedSurnames.size(), 0);
    }
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
  public void testUseOfIndex() {
    var resultSet = executeQuery("select * from Profile where nick = 'Jay'");

    Assert.assertFalse(resultSet.isEmpty());

    Entity record;
    for (var entries : resultSet) {
      record = entries.asEntity();
      Assert.assertTrue(record.<String>getProperty("name").equalsIgnoreCase("Jay"));
    }
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
  public void testIndexEntries() {
    checkEmbeddedDB();

    var resultSet = executeQuery("select * from Profile where nick is not null");

    var idx =
        session.getMetadata().getIndexManagerInternal().getIndex(session, "Profile.nick");

    Assert.assertEquals(idx.getInternal().size(session), resultSet.size());
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
  public void testIndexSize() {
    checkEmbeddedDB();

    var resultSet = executeQuery("select * from Profile where nick is not null");

    var profileSize = resultSet.size();

    Assert.assertEquals(
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "Profile.nick")
            .getInternal()
            .size(session),
        profileSize);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var profile = session.newEntity("Profile");
      profile.setProperty("nick", "Yay-" + i);
      profile.setProperty("name", "Jay");
      profile.setProperty("surname", "Miner");
      session.commit();

      profileSize++;
      try (var stream =
          session
              .getMetadata()
              .getIndexManagerInternal()
              .getIndex(session, "Profile.nick")
              .getInternal()
              .getRids(session, "Yay-" + i)) {
        Assert.assertTrue(stream.findAny().isPresent());
      }
    }
  }

  @Test(dependsOnMethods = "testUseOfIndex")
  public void testChangeOfIndexToNotUnique() {
    dropIndexes();

    session
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty(session, "nick")
        .createIndex(session, INDEX_TYPE.NOTUNIQUE);
  }

  private void dropIndexes() {
    if (remoteDB) {
      session.command("drop index " + "Profile" + "." + "nick").close();
    } else {
      for (var indexName : session.getMetadata().getSchema().getClassInternal("Profile")
          .getPropertyInternal(session, "nick").getAllIndexes(session)) {
        session.getMetadata().getIndexManagerInternal().dropIndex(session, indexName);
      }
    }
  }

  @Test(dependsOnMethods = "testChangeOfIndexToNotUnique")
  public void testDuplicatedIndexOnNotUnique() {
    session.begin();
    var nickNolte = session.newEntity("Profile");
    nickNolte.setProperty("nick", "Jay");
    nickNolte.setProperty("name", "Nick");
    nickNolte.setProperty("surname", "Nolte");

    session.commit();
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnNotUnique")
  public void testChangeOfIndexToUnique() {
    try {
      dropIndexes();
      session
          .getMetadata()
          .getSchema()
          .getClass("Profile")
          .getProperty(session, "nick")
          .createIndex(session, INDEX_TYPE.UNIQUE);
      Assert.fail();
    } catch (RecordDuplicatedException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMajorSelect() {
    if (session.isRemote()) {
      return;
    }

    try (var resultSet =
        session.query("select * from Profile where nick > 'ZZZJayLongNickIndex3'")) {
      assertIndexUsage(resultSet);
      final List<String> expectedNicks =
          new ArrayList<>(Arrays.asList("ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 2);
      for (var profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMajorEqualsSelect() {
    if (session.isRemote()) {
      return;
    }

    try (var resultSet =
        session.query("select * from Profile where nick >= 'ZZZJayLongNickIndex3'")) {
      assertIndexUsage(resultSet);
      final List<String> expectedNicks =
          new ArrayList<>(
              Arrays.asList(
                  "ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (var profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMinorSelect() {
    if (session.isRemote()) {
      return;
    }

    try (var resultSet = session.query("select * from Profile where nick < '002'")) {
      assertIndexUsage(resultSet);
      final List<String> expectedNicks = new ArrayList<>(Arrays.asList("000", "001"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 2);
      for (var profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMinorEqualsSelect() {
    if (session.isRemote()) {
      return;
    }

    try (var resultSet = session.query("select * from Profile where nick <= '002'")) {
      final List<String> expectedNicks = new ArrayList<>(Arrays.asList("000", "001", "002"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (var profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments", enabled = false)
  public void testIndexBetweenSelect() {
    if (session.isRemote()) {
      return;
    }

    var query = "select * from Profile where nick between '001' and '004'";
    try (var resultSet = session.query(query)) {
      assertIndexUsage(resultSet);
      final List<String> expectedNicks = new ArrayList<>(Arrays.asList("001", "002", "003", "004"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 4);
      for (var profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments", enabled = false)
  public void testIndexInComplexSelectOne() {
    if (session.isRemote()) {
      return;
    }

    try (var resultSet =
        session.query(
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
      for (var profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments", enabled = false)
  public void testIndexInComplexSelectTwo() {
    if (session.isRemote()) {
      return;
    }

    try (var resultSet =
        session.query(
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
      for (var profile : result) {
        expectedNicks.remove(profile.<String>getProperty("nick"));
      }

      Assert.assertEquals(expectedNicks.size(), 0);
    }
  }

  public void populateIndexDocuments() {
    for (var i = 0; i <= 5; i++) {
      session.begin();
      final var profile = session.newEntity("Profile");
      profile.setProperty("nick", "ZZZJayLongNickIndex" + i);
      profile.setProperty("name", "NickIndex" + i);
      profile.setProperty("surname", "NolteIndex" + i);
      session.commit();
    }

    for (var i = 0; i <= 5; i++) {
      session.begin();
      final var profile = session.newEntity("Profile");
      profile.setProperty("nick", "00" + i);
      profile.setProperty("name", "NickIndex" + i);
      profile.setProperty("surname", "NolteIndex" + i);
      session.commit();
    }
  }

  @Test(dependsOnMethods = "testChangeOfIndexToUnique")
  public void removeNotUniqueIndexOnNick() {
    if (remoteDB) {
      return;
    }

    dropIndexes();
  }

  @Test(dependsOnMethods = "removeNotUniqueIndexOnNick")
  public void testQueryingWithoutNickIndex() {
    if (!remoteDB) {
      Assert.assertFalse(
          session.getMetadata().getSchema().getClassInternal("Profile")
              .getInvolvedIndexes(session, "name").isEmpty());

      Assert.assertTrue(
          session.getMetadata().getSchema().getClassInternal("Profile")
              .getInvolvedIndexes(session, "nick")
              .isEmpty());
    }

    List<EntityImpl> result =
        session
            .command(new SQLSynchQuery<EntityImpl>("SELECT FROM Profile WHERE nick = 'Jay'"))
            .execute(session);
    Assert.assertEquals(result.size(), 2);

    result =
        session
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "SELECT FROM Profile WHERE nick = 'Jay' AND name = 'Jay'"))
            .execute(session);
    Assert.assertEquals(result.size(), 1);

    result =
        session
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "SELECT FROM Profile WHERE nick = 'Jay' AND name = 'Nick'"))
            .execute(session);
    Assert.assertEquals(result.size(), 1);
  }

  @Test(dependsOnMethods = "testQueryingWithoutNickIndex")
  public void createNotUniqueIndexOnNick() {
    session
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty(session, "nick")
        .createIndex(session, INDEX_TYPE.NOTUNIQUE);
  }

  @Test(dependsOnMethods = {"createNotUniqueIndexOnNick", "populateIndexDocuments"})
  public void testIndexInNotUniqueIndex() {
    if (!remoteDB) {
      Assert.assertEquals(
          session.getClassInternal("Profile").
              getInvolvedIndexesInternal(session, "nick").iterator()
              .next().getType(),
          SchemaClass.INDEX_TYPE.NOTUNIQUE.toString());
    }

    try (var resultSet =
        session.query(
            "SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0'"
                + " ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']")) {
      final List<String> expectedSurnames =
          new ArrayList<>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

      var result = resultSet.entityStream().toList();
      Assert.assertEquals(result.size(), 3);
      for (final var profile : result) {
        expectedSurnames.remove(profile.<String>getProperty("surname"));
      }

      Assert.assertEquals(expectedSurnames.size(), 0);
    }
  }

  @Test
  public void indexLinks() {
    checkEmbeddedDB();

    session
        .getMetadata()
        .getSchema()
        .getClass("Whiz")
        .getProperty(session, "account")
        .createIndex(session, INDEX_TYPE.NOTUNIQUE);

    var resultSet = executeQuery("select * from Account limit 1");
    final var idx =
        session.getMetadata().getIndexManagerInternal().getIndex(session, "Whiz.account");

    for (var i = 0; i < 5; i++) {
      session.begin();
      final var whiz = ((EntityImpl) session.newEntity("Whiz"));

      whiz.field("id", i);
      whiz.field("text", "This is a test");
      whiz.field("account", resultSet.getFirst().asEntity().getIdentity());

      session.commit();
    }

    Assert.assertEquals(idx.getInternal().size(session), 5);

    var indexedResult =
        executeQuery("select * from Whiz where account = ?",
            resultSet.getFirst().getIdentity());
    Assert.assertEquals(indexedResult.size(), 5);

    session.begin();
    for (var res : indexedResult) {
      res.asEntity().delete();
    }

    var whiz = session.newEntity("Whiz");
    whiz.setProperty("id", 100);
    whiz.setProperty("text", "This is a test!");
    whiz.setProperty("account", ((EntityImpl) session.newEntity("Company")).field("id", 9999));
    session.commit();

    session.begin();
    whiz = session.bindToSession(whiz);
    Assert.assertTrue(((EntityImpl) whiz.getProperty("account")).getIdentity().isValid());
    ((EntityImpl) whiz.getProperty("account")).delete();
    whiz.delete();
    session.commit();
  }

  public void linkedIndexedProperty() {
    try (var db = acquireSession()) {
      if (!db.getMetadata().getSchema().existsClass("TestClass")) {
        var testClass =
            db.getMetadata().getSchema().createClass("TestClass", 1, (SchemaClass[]) null);
        var testLinkClass =
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
      var testLinkClassDocument = ((EntityImpl) db.newEntity("TestLinkClass"));
      testLinkClassDocument.field("testString", "Test Link Class 1");
      testLinkClassDocument.field("testBoolean", true);
      testClassDocument.field("testLink", testLinkClassDocument);

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
    try (var db = acquireSession()) {
      db.begin();
      EntityImpl testClassDocument = db.newInstance("TestClass");
      testClassDocument.field("name", "Test Class 2");
      var testLinkClassDocument = ((EntityImpl) db.newEntity("TestLinkClass"));
      testLinkClassDocument.field("testString", "Test Link Class 2");
      testLinkClassDocument.field("testBoolean", true);
      testClassDocument.field("testLink", testLinkClassDocument);

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

    try (var db = acquireSession()) {
      if (!db.getMetadata().getSchema().existsClass("MyFruit")) {
        var fruitClass = db.getMetadata().getSchema()
            .createClass("MyFruit", 1, (SchemaClass[]) null);
        fruitClass.createProperty(db, "name", PropertyType.STRING);
        fruitClass.createProperty(db, "color", PropertyType.STRING);

        db.getMetadata()
            .getSchema()
            .getClass("MyFruit")
            .getProperty(session, "name")
            .createIndex(db, INDEX_TYPE.UNIQUE);

        db.getMetadata()
            .getSchema()
            .getClass("MyFruit")
            .getProperty(session, "color")
            .createIndex(db, INDEX_TYPE.NOTUNIQUE);
      }

      long expectedIndexSize = 0;

      final var passCount = 10;
      final var chunkSize = 10;

      for (var pass = 0; pass < passCount; pass++) {
        List<EntityImpl> recordsToDelete = new ArrayList<>();
        db.begin();
        for (var i = 0; i < chunkSize; i++) {
          var d =
              ((EntityImpl) db.newEntity("MyFruit"))
                  .field("name", "ABC" + pass + 'K' + i)
                  .field("color", "FOO" + pass);

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
        for (final var recordToDelete : recordsToDelete) {
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
    try (var db = acquireSession()) {
      if (!db.getMetadata().getSchema().existsClass("IndexTestTerm")) {
        final var termClass =
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

      db.commit();

      try (var stream =
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

    var db = acquireSession();
    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexTest")) {
      final var termClass =
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
    var docOne = ((EntityImpl) db.newEntity("TransactionUniqueIndexTest"));
    docOne.field("label", "A");

    db.commit();

    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "idxTransactionUniqueIndexTest");
    Assert.assertEquals(index.getInternal().size(this.session), 1);

    db.begin();
    try {
      var docTwo = ((EntityImpl) db.newEntity("TransactionUniqueIndexTest"));
      docTwo.field("label", "A");

      db.commit();
      Assert.fail();
    } catch (RecordDuplicatedException ignored) {
    }

    Assert.assertEquals(index.getInternal().size(this.session), 1);
  }

  @Test(dependsOnMethods = "testTransactionUniqueIndexTestOne")
  public void testTransactionUniqueIndexTestTwo() {
    checkEmbeddedDB();

    var db = acquireSession();
    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexTest")) {
      final var termClass =
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
    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "idxTransactionUniqueIndexTest");
    Assert.assertEquals(index.getInternal().size(this.session), 1);

    db.begin();
    try {
      var docOne = ((EntityImpl) db.newEntity("TransactionUniqueIndexTest"));
      docOne.field("label", "B");

      var docTwo = ((EntityImpl) db.newEntity("TransactionUniqueIndexTest"));
      docTwo.field("label", "B");

      db.commit();
      Assert.fail();
    } catch (RecordDuplicatedException oie) {
      db.rollback();
    }

    Assert.assertEquals(index.getInternal().size(this.session), 1);
  }

  public void testTransactionUniqueIndexTestWithDotNameOne() {
    checkEmbeddedDB();

    var db = acquireSession();
    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexWithDotTest")) {
      final var termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexWithDotTest", 1, (SchemaClass[]) null);
      termClass.createProperty(db, "label", PropertyType.STRING).createIndex(db, INDEX_TYPE.UNIQUE);
    }

    db.begin();
    var docOne = ((EntityImpl) db.newEntity("TransactionUniqueIndexWithDotTest"));
    docOne.field("label", "A");

    db.commit();

    final var index =
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "TransactionUniqueIndexWithDotTest.label");
    Assert.assertEquals(index.getInternal().size(this.session), 1);

    var countClassBefore = db.countClass("TransactionUniqueIndexWithDotTest");
    db.begin();
    try {
      var docTwo = ((EntityImpl) db.newEntity("TransactionUniqueIndexWithDotTest"));
      docTwo.field("label", "A");

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

    var db = acquireSession();
    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexWithDotTest")) {
      final var termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexWithDotTest", 1, (SchemaClass[]) null);
      termClass.createProperty(db, "label", PropertyType.STRING)
          .createIndex(this.session, INDEX_TYPE.UNIQUE);
    }

    final var index =
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "TransactionUniqueIndexWithDotTest.label");
    Assert.assertEquals(index.getInternal().size(this.session), 1);

    db.begin();
    try {
      var docOne = ((EntityImpl) db.newEntity("TransactionUniqueIndexWithDotTest"));
      docOne.field("label", "B");

      var docTwo = ((EntityImpl) db.newEntity("TransactionUniqueIndexWithDotTest"));
      docTwo.field("label", "B");

      db.commit();
      Assert.fail();
    } catch (RecordDuplicatedException oie) {
      db.rollback();
    }

    Assert.assertEquals(index.getInternal().size(this.session), 1);
  }

  @Test(dependsOnMethods = "linkedIndexedProperty")
  public void testIndexRemoval() {
    checkEmbeddedDB();

    final var index = getIndex("Profile.nick");

    Iterator<RawPair<Object, RID>> streamIterator;
    Object key;
    try (var stream = index.getInternal().stream(session)) {
      streamIterator = stream.iterator();
      Assert.assertTrue(streamIterator.hasNext());

      var pair = streamIterator.next();
      key = pair.first;

      session.begin();
      pair.second.getRecord(session).delete();
      session.commit();
    }

    try (var stream = index.getInternal().getRids(session, key)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  public void createInheritanceIndex() {
    try (var db = acquireSession()) {
      if (!db.getMetadata().getSchema().existsClass("BaseTestClass")) {
        var baseClass =
            db.getMetadata().getSchema().createClass("BaseTestClass", 1, (SchemaClass[]) null);
        var childClass =
            db.getMetadata().getSchema().createClass("ChildTestClass", 1, (SchemaClass[]) null);
        var anotherChildClass =
            db.getMetadata().getSchema()
                .createClass("AnotherChildTestClass", 1, (SchemaClass[]) null);

        if (!baseClass.isSuperClassOf(session, childClass)) {
          childClass.setSuperClass(db, baseClass);
        }
        if (!baseClass.isSuperClassOf(session, anotherChildClass)) {
          anotherChildClass.setSuperClass(db, baseClass);
        }

        baseClass
            .createProperty(db, "testParentProperty", PropertyType.LONG)
            .createIndex(db, INDEX_TYPE.NOTUNIQUE);
      }

      db.begin();
      EntityImpl childClassDocument = db.newInstance("ChildTestClass");
      childClassDocument.field("testParentProperty", 10L);

      EntityImpl anotherChildClassDocument = db.newInstance("AnotherChildTestClass");
      anotherChildClassDocument.field("testParentProperty", 11L);

      db.commit();

      Assert.assertNotEquals(
          childClassDocument.getIdentity(), new RecordId(-1, RID.CLUSTER_POS_INVALID));
      Assert.assertNotEquals(
          anotherChildClassDocument.getIdentity(), new RecordId(-1, RID.CLUSTER_POS_INVALID));
    }
  }

  @Test(dependsOnMethods = "createInheritanceIndex")
  public void testIndexReturnOnlySpecifiedClass() {

    try (var result =
        session.command("select * from ChildTestClass where testParentProperty = 10")) {

      Assert.assertEquals(result.next().<Object>getProperty("testParentProperty"), 10L);
      Assert.assertFalse(result.hasNext());
    }

    try (var result =
        session.command("select * from AnotherChildTestClass where testParentProperty = 11")) {
      Assert.assertEquals(result.next().<Object>getProperty("testParentProperty"), 11L);
      Assert.assertFalse(result.hasNext());
    }
  }

  public void testNotUniqueIndexKeySize() {
    checkEmbeddedDB();

    final Schema schema = session.getMetadata().getSchema();
    var cls = schema.createClass("IndexNotUniqueIndexKeySize");
    cls.createProperty(session, "value", PropertyType.INTEGER);
    cls.createIndex(session, "IndexNotUniqueIndexKeySizeIndex", INDEX_TYPE.NOTUNIQUE, "value");

    var idxManager = session.getMetadata().getIndexManagerInternal();

    final var idx = idxManager.getIndex(session, "IndexNotUniqueIndexKeySizeIndex");

    final Set<Integer> keys = new HashSet<>();
    for (var i = 1; i < 100; i++) {
      final Integer key = (int) Math.log(i);

      session.begin();
      final var doc = ((EntityImpl) session.newEntity("IndexNotUniqueIndexKeySize"));
      doc.field("value", key);

      session.commit();

      keys.add(key);
    }

    try (var stream = idx.getInternal().stream(session)) {
      Assert.assertEquals(stream.map((pair) -> pair.first).distinct().count(), keys.size());
    }
  }

  public void testNotUniqueIndexSize() {
    checkEmbeddedDB();

    final Schema schema = session.getMetadata().getSchema();
    var cls = schema.createClass("IndexNotUniqueIndexSize");
    cls.createProperty(session, "value", PropertyType.INTEGER);
    cls.createIndex(session, "IndexNotUniqueIndexSizeIndex", INDEX_TYPE.NOTUNIQUE, "value");

    var idxManager = session.getMetadata().getIndexManagerInternal();
    final var idx = idxManager.getIndex(session, "IndexNotUniqueIndexSizeIndex");

    for (var i = 1; i < 100; i++) {
      final Integer key = (int) Math.log(i);

      session.begin();
      final var doc = ((EntityImpl) session.newEntity("IndexNotUniqueIndexSize"));
      doc.field("value", key);

      session.commit();
    }

    Assert.assertEquals(idx.getInternal().size(session), 99);
  }

  @Test
  public void testIndexRebuildDuringNonProxiedObjectDelete() {
    checkEmbeddedDB();

    session.begin();
    var profile = session.newEntity("Profile");
    profile.setProperty("nick", "NonProxiedObjectToDelete");
    profile.setProperty("name", "NonProxiedObjectToDelete");
    profile.setProperty("surname", "NonProxiedObjectToDelete");
    profile = profile;
    session.commit();

    var idxManager = session.getMetadata().getIndexManagerInternal();
    var nickIndex = idxManager.getIndex(session, "Profile.nick");

    try (var stream = nickIndex.getInternal()
        .getRids(session, "NonProxiedObjectToDelete")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    session.begin();
    final Entity loadedProfile = session.load(profile.getIdentity());
    session.delete(loadedProfile);
    session.commit();

    try (var stream = nickIndex.getInternal()
        .getRids(session, "NonProxiedObjectToDelete")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test(dependsOnMethods = "testIndexRebuildDuringNonProxiedObjectDelete")
  public void testIndexRebuildDuringDetachAllNonProxiedObjectDelete() {
    checkEmbeddedDB();

    session.begin();
    var profile = session.newEntity("Profile");
    profile.setProperty("nick", "NonProxiedObjectToDelete");
    profile.setProperty("name", "NonProxiedObjectToDelete");
    profile.setProperty("surname", "NonProxiedObjectToDelete");
    profile = profile;
    session.commit();

    var idxManager = session.getMetadata().getIndexManagerInternal();
    var nickIndex = idxManager.getIndex(session, "Profile.nick");

    try (var stream = nickIndex.getInternal()
        .getRids(session, "NonProxiedObjectToDelete")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    final Entity loadedProfile = session.load(profile.getIdentity());
    session.begin();
    session.delete(session.bindToSession(loadedProfile));
    session.commit();

    try (var stream = nickIndex.getInternal()
        .getRids(session, "NonProxiedObjectToDelete")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test(dependsOnMethods = "testIndexRebuildDuringDetachAllNonProxiedObjectDelete")
  public void testRestoreUniqueIndex() {
    dropIndexes();
    session
        .command(
            "CREATE INDEX Profile.nick on Profile (nick) UNIQUE METADATA {ignoreNullValues: true}")
        .close();
    session.getMetadata().reload();
  }

  @Test
  public void testIndexInCompositeQuery() {
    var classOne =
        session.getMetadata().getSchema()
            .createClass("CompoundSQLIndexTest1", 1, (SchemaClass[]) null);
    var classTwo =
        session.getMetadata().getSchema()
            .createClass("CompoundSQLIndexTest2", 1, (SchemaClass[]) null);

    classTwo.createProperty(session, "address", PropertyType.LINK, classOne);

    classTwo.createIndex(session, "CompoundSQLIndexTestIndex", INDEX_TYPE.UNIQUE, "address");

    session.begin();
    var docOne = ((EntityImpl) session.newEntity("CompoundSQLIndexTest1"));
    docOne.field("city", "Montreal");

    var docTwo = ((EntityImpl) session.newEntity("CompoundSQLIndexTest2"));
    docTwo.field("address", docOne);

    session.commit();

    var result =
        executeQuery(
            "select from CompoundSQLIndexTest2 where address in (select from"
                + " CompoundSQLIndexTest1 where city='Montreal')");
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.getFirst().getIdentity(), docTwo.getIdentity());
  }

  public void testIndexWithLimitAndOffset() {
    final var schema = session.getSchema();
    final var indexWithLimitAndOffset =
        schema.createClass("IndexWithLimitAndOffsetClass", 1, (SchemaClass[]) null);
    indexWithLimitAndOffset.createProperty(session, "val", PropertyType.INTEGER);
    indexWithLimitAndOffset.createProperty(session, "index", PropertyType.INTEGER);

    session
        .command(
            "create index IndexWithLimitAndOffset on IndexWithLimitAndOffsetClass (val) notunique")
        .close();

    for (var i = 0; i < 30; i++) {
      session.begin();
      final var document = ((EntityImpl) session.newEntity("IndexWithLimitAndOffsetClass"));
      document.field("val", i / 10);
      document.field("index", i);

      session.commit();
    }

    var resultSet =
        executeQuery("select from IndexWithLimitAndOffsetClass where val = 1 offset 5 limit 2");
    Assert.assertEquals(resultSet.size(), 2);

    for (var i = 0; i < 2; i++) {
      var result = resultSet.get(i);
      Assert.assertEquals(result.<Object>getProperty("val"), 1);
      Assert.assertEquals(result.<Object>getProperty("index"), 15 + i);
    }
  }

  public void testNullIndexKeysSupport() {
    final var schema = session.getSchema();
    final var clazz = schema.createClass("NullIndexKeysSupport", 1, (SchemaClass[]) null);
    clazz.createProperty(session, "nullField", PropertyType.STRING);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(session,
        "NullIndexKeysSupportIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"nullField"});
    for (var i = 0; i < 20; i++) {
      session.begin();
      if (i % 5 == 0) {
        var document = ((EntityImpl) session.newEntity("NullIndexKeysSupport"));
        document.field("nullField", null);

      } else {
        var document = ((EntityImpl) session.newEntity("NullIndexKeysSupport"));
        document.field("nullField", "val" + i);

      }
      session.commit();
    }

    var resultSet =
        executeQuery("select from NullIndexKeysSupport where nullField = 'val3'");
    Assert.assertEquals(resultSet.size(), 1);

    Assert.assertEquals(resultSet.getFirst().getProperty("nullField"), "val3");

    final var query = "select from NullIndexKeysSupport where nullField is null";
    resultSet = executeQuery("select from NullIndexKeysSupport where nullField is null");

    Assert.assertEquals(resultSet.size(), 4);
    for (var result : resultSet) {
      Assert.assertNull(result.getProperty("nullField"));
    }

    final EntityImpl explain = session.command(new CommandSQL("explain " + query))
        .execute(session);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("NullIndexKeysSupportIndex"));
  }

  public void testNullHashIndexKeysSupport() {
    final var schema = session.getSchema();
    final var clazz = schema.createClass("NullHashIndexKeysSupport", 1,
        (SchemaClass[]) null);
    clazz.createProperty(session, "nullField", PropertyType.STRING);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(session,
        "NullHashIndexKeysSupportIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"nullField"});
    for (var i = 0; i < 20; i++) {
      session.begin();
      if (i % 5 == 0) {
        var document = ((EntityImpl) session.newEntity("NullHashIndexKeysSupport"));
        document.field("nullField", null);

      } else {
        var document = ((EntityImpl) session.newEntity("NullHashIndexKeysSupport"));
        document.field("nullField", "val" + i);

      }
      session.commit();
    }

    List<EntityImpl> result =
        session.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullHashIndexKeysSupport where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.getFirst().field("nullField"), "val3");

    final var query = "select from NullHashIndexKeysSupport where nullField is null";
    result =
        session.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullHashIndexKeysSupport where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (var document : result) {
      Assert.assertNull(document.field("nullField"));
    }

    final EntityImpl explain = session.command(new CommandSQL("explain " + query))
        .execute(session);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("NullHashIndexKeysSupportIndex"));
  }

  public void testNullIndexKeysSupportInTx() {
    final Schema schema = session.getMetadata().getSchema();
    final var clazz = schema.createClass("NullIndexKeysSupportInTx", 1,
        (SchemaClass[]) null);
    clazz.createProperty(session, "nullField", PropertyType.STRING);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(session,
        "NullIndexKeysSupportInTxIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"nullField"});

    session.begin();

    for (var i = 0; i < 20; i++) {
      if (i % 5 == 0) {
        var document = ((EntityImpl) session.newEntity("NullIndexKeysSupportInTx"));
        document.field("nullField", null);

      } else {
        var document = ((EntityImpl) session.newEntity("NullIndexKeysSupportInTx"));
        document.field("nullField", "val" + i);

      }
    }

    session.commit();

    List<EntityImpl> result =
        session.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullIndexKeysSupportInTx where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.getFirst().field("nullField"), "val3");

    final var query = "select from NullIndexKeysSupportInTx where nullField is null";
    result =
        session.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullIndexKeysSupportInTx where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (var document : result) {
      Assert.assertNull(document.field("nullField"));
    }

    final EntityImpl explain = session.command(new CommandSQL("explain " + query))
        .execute(session);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("NullIndexKeysSupportInTxIndex"));
  }

  public void testNullIndexKeysSupportInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final var schema = session.getSchema();
    final var clazz = schema.createClass("NullIndexKeysSupportInMiddleTx", 1,
        (SchemaClass[]) null);
    clazz.createProperty(session, "nullField", PropertyType.STRING);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(session,
        "NullIndexKeysSupportInMiddleTxIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"nullField"});

    session.begin();

    for (var i = 0; i < 20; i++) {
      if (i % 5 == 0) {
        var document = ((EntityImpl) session.newEntity("NullIndexKeysSupportInMiddleTx"));
        document.field("nullField", null);

      } else {
        var document = ((EntityImpl) session.newEntity("NullIndexKeysSupportInMiddleTx"));
        document.field("nullField", "val" + i);

      }
    }

    List<EntityImpl> result =
        session.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullIndexKeysSupportInMiddleTx where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.getFirst().field("nullField"), "val3");

    final var query = "select from NullIndexKeysSupportInMiddleTx where nullField is null";
    result =
        session.query(
            new SQLSynchQuery<EntityImpl>(
                "select from NullIndexKeysSupportInMiddleTx where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (var document : result) {
      Assert.assertNull(document.field("nullField"));
    }

    final EntityImpl explain = session.command(new CommandSQL("explain " + query))
        .execute(session);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("NullIndexKeysSupportInMiddleTxIndex"));

    session.commit();
  }

  public void testCreateIndexAbstractClass() {
    final var schema = session.getSchema();

    var abstractClass = schema.createAbstractClass("TestCreateIndexAbstractClass");
    abstractClass
        .createProperty(session, "value", PropertyType.STRING)
        .setMandatory(session, true)
        .createIndex(session, INDEX_TYPE.UNIQUE);

    schema.createClass("TestCreateIndexAbstractClassChildOne", abstractClass);
    schema.createClass("TestCreateIndexAbstractClassChildTwo", abstractClass);

    session.begin();
    var docOne = ((EntityImpl) session.newEntity("TestCreateIndexAbstractClassChildOne"));
    docOne.field("value", "val1");

    var docTwo = ((EntityImpl) session.newEntity("TestCreateIndexAbstractClassChildTwo"));
    docTwo.field("value", "val2");

    session.commit();

    final var queryOne = "select from TestCreateIndexAbstractClass where value = 'val1'";

    var resultOne = executeQuery(queryOne);
    Assert.assertEquals(resultOne.size(), 1);
    Assert.assertEquals(resultOne.getFirst().getIdentity(), docOne.getIdentity());

    try (var result = session.command("explain " + queryOne)) {
      var explain = result.next();
      Assert.assertTrue(
          explain
              .<String>getProperty("executionPlanAsString")
              .contains("FETCH FROM INDEX TestCreateIndexAbstractClass.value"));

      final var queryTwo = "select from TestCreateIndexAbstractClass where value = 'val2'";

      var resultTwo = executeQuery(queryTwo);
      Assert.assertEquals(resultTwo.size(), 1);
      Assert.assertEquals(resultTwo.getFirst().getIdentity(), docTwo.getIdentity());

      explain = session.command(new CommandSQL("explain " + queryTwo)).execute(session);
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

    final Schema schema = session.getMetadata().getSchema();
    var clazz =
        schema.createClass("ValuesContainerIsRemovedIfIndexIsRemovedClass", 1,
            (SchemaClass[]) null);
    clazz.createProperty(session, "val", PropertyType.STRING);

    session
        .command(
            "create index ValuesContainerIsRemovedIfIndexIsRemovedIndex on"
                + " ValuesContainerIsRemovedIfIndexIsRemovedClass (val) notunique")
        .close();

    for (var i = 0; i < 10; i++) {
      for (var j = 0; j < 100; j++) {
        session.begin();
        var document = ((EntityImpl) session.newEntity(
            "ValuesContainerIsRemovedIfIndexIsRemovedClass"));
        document.field("val", "value" + i);

        session.commit();
      }
    }

    final var storageLocalAbstract =
        (AbstractPaginatedStorage)
            ((DatabaseSessionInternal) session.getUnderlying()).getStorage();

    final var writeCache = storageLocalAbstract.getWriteCache();
    Assert.assertTrue(writeCache.exists("ValuesContainerIsRemovedIfIndexIsRemovedIndex.irs"));
    session.command("drop index ValuesContainerIsRemovedIfIndexIsRemovedIndex").close();
    Assert.assertFalse(writeCache.exists("ValuesContainerIsRemovedIfIndexIsRemovedIndex.irs"));
  }

  public void testPreservingIdentityInIndexTx() {
    checkEmbeddedDB();
    if (!session.getMetadata().getSchema().existsClass("PreservingIdentityInIndexTxParent")) {
      session.createVertexClass("PreservingIdentityInIndexTxParent");
    }
    if (!session.getMetadata().getSchema().existsClass("PreservingIdentityInIndexTxEdge")) {
      session.createEdgeClass("PreservingIdentityInIndexTxEdge");
    }
    var fieldClass = (SchemaClassInternal) session.getClass("PreservingIdentityInIndexTxChild");
    if (fieldClass == null) {
      fieldClass = (SchemaClassInternal) session.createVertexClass(
          "PreservingIdentityInIndexTxChild");
      fieldClass.createProperty(session, "name", PropertyType.STRING);
      fieldClass.createProperty(session, "in_field", PropertyType.LINK);
      fieldClass.createIndex(session, "nameParentIndex", INDEX_TYPE.NOTUNIQUE, "in_field", "name");
    }

    session.begin();
    var parent = session.newVertex("PreservingIdentityInIndexTxParent");
    var child = session.newVertex("PreservingIdentityInIndexTxChild");
    session.newStatefulEdge(parent, child, "PreservingIdentityInIndexTxEdge");
    child.setProperty("name", "pokus");

    var parent2 = session.newVertex("PreservingIdentityInIndexTxParent");
    var child2 = session.newVertex("PreservingIdentityInIndexTxChild");
    session.newStatefulEdge(parent2, child2, "preservingIdentityInIndexTxEdge");
    child2.setProperty("name", "pokus2");
    session.commit();

    {
      fieldClass = session.getClassInternal("PreservingIdentityInIndexTxChild");
      var index = fieldClass.getClassIndex(session, "nameParentIndex");
      var key = new CompositeKey(parent.getIdentity(), "pokus");

      Collection<RID> h;
      try (var stream = index.getInternal().getRids(session, key)) {
        h = stream.toList();
      }
      for (var o : h) {
        Assert.assertNotNull(session.load(o));
      }
    }

    {
      fieldClass = (SchemaClassInternal) session.getClass("PreservingIdentityInIndexTxChild");
      var index = fieldClass.getClassIndex(session, "nameParentIndex");
      var key = new CompositeKey(parent2.getIdentity(), "pokus2");

      Collection<RID> h;
      try (var stream = index.getInternal().getRids(session, key)) {
        h = stream.toList();
      }
      for (var o : h) {
        Assert.assertNotNull(session.load(o));
      }
    }

    session.begin();
    session.delete(session.bindToSession(parent));
    session.delete(session.bindToSession(child));

    session.delete(session.bindToSession(parent2));
    session.delete(session.bindToSession(child2));
    session.commit();
  }

  public void testEmptyNotUniqueIndex() {
    checkEmbeddedDB();

    var emptyNotUniqueIndexClazz =
        session
            .getMetadata()
            .getSchema()
            .createClass("EmptyNotUniqueIndexTest", 1, (SchemaClass[]) null);
    emptyNotUniqueIndexClazz.createProperty(session, "prop", PropertyType.STRING);

    emptyNotUniqueIndexClazz.createIndex(session,
        "EmptyNotUniqueIndexTestIndex", INDEX_TYPE.NOTUNIQUE, "prop");
    final var notUniqueIndex = session.getIndex("EmptyNotUniqueIndexTestIndex");

    session.begin();
    var document = ((EntityImpl) session.newEntity("EmptyNotUniqueIndexTest"));
    document.field("prop", "keyOne");

    document = ((EntityImpl) session.newEntity("EmptyNotUniqueIndexTest"));
    document.field("prop", "keyTwo");

    session.commit();

    try (var stream = notUniqueIndex.getInternal().getRids(session, "RandomKeyOne")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (var stream = notUniqueIndex.getInternal().getRids(session, "keyOne")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    try (var stream = notUniqueIndex.getInternal().getRids(session, "RandomKeyTwo")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (var stream = notUniqueIndex.getInternal().getRids(session, "keyTwo")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  public void testNullIteration() {
    var v = session.getSchema().getClass("V");
    var testNullIteration =
        session.getMetadata().getSchema().createClass("NullIterationTest", v);
    testNullIteration.createProperty(session, "name", PropertyType.STRING);
    testNullIteration.createProperty(session, "birth", PropertyType.DATETIME);

    session.begin();
    session
        .command("CREATE VERTEX NullIterationTest SET name = 'Andrew', birth = sysdate()")
        .close();
    session
        .command("CREATE VERTEX NullIterationTest SET name = 'Marcel', birth = sysdate()")
        .close();
    session.command("CREATE VERTEX NullIterationTest SET name = 'Olivier'").close();
    session.commit();

    var metadata = Map.of("ignoreNullValues", false);

    testNullIteration.createIndex(session,
        "NullIterationTestIndex",
        INDEX_TYPE.NOTUNIQUE.name(),
        null,
        metadata, new String[]{"birth"});

    var result = session.query("SELECT FROM NullIterationTest ORDER BY birth ASC");
    Assert.assertEquals(result.stream().count(), 3);

    result = session.query("SELECT FROM NullIterationTest ORDER BY birth DESC");
    Assert.assertEquals(result.stream().count(), 3);

    result = session.query("SELECT FROM NullIterationTest");
    Assert.assertEquals(result.stream().count(), 3);
  }

  public void testMultikeyWithoutFieldAndNullSupport() {
    checkEmbeddedDB();

    // generates stubs for index
    session.begin();
    var doc1 = ((EntityImpl) session.newEntity());

    var doc2 = ((EntityImpl) session.newEntity());

    var doc3 = ((EntityImpl) session.newEntity());

    var doc4 = ((EntityImpl) session.newEntity());

    session.commit();

    final RID rid1 = doc1.getIdentity();
    final RID rid2 = doc2.getIdentity();
    final RID rid3 = doc3.getIdentity();
    final RID rid4 = doc4.getIdentity();
    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("TestMultikeyWithoutField");

    clazz.createProperty(session, "state", PropertyType.BYTE);
    clazz.createProperty(session, "users", PropertyType.LINKSET);
    clazz.createProperty(session, "time", PropertyType.LONG);
    clazz.createProperty(session, "reg", PropertyType.LONG);
    clazz.createProperty(session, "no", PropertyType.INTEGER);

    var mt = Map.of("ignoreNullValues", false);
    clazz.createIndex(session,
        "MultikeyWithoutFieldIndex",
        INDEX_TYPE.UNIQUE.toString(),
        null,
        mt, new String[]{"state", "users", "time", "reg", "no"});

    session.begin();
    var document = ((EntityImpl) session.newEntity("TestMultikeyWithoutField"));
    document.field("state", (byte) 1);

    Set<RID> users = new HashSet<>();
    users.add(rid1);
    users.add(rid2);

    document.field("users", users);
    document.field("time", 12L);
    document.field("reg", 14L);
    document.field("no", 12);

    session.commit();

    var index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(session), 2);

    // we support first and last keys check only for embedded storage
    // we support first and last keys check only for embedded storage
    if (!(session.isRemote())) {
      try (var keyStream = index.getInternal().keyStream()) {
        if (rid1.compareTo(rid2) < 0) {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid1, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
        }
      }
      try (var descStream = index.getInternal().descStream(session)) {
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

    session.close();
    session = acquireSession();

    session.begin();
    document = session.load(rid);

    users = document.field("users");
    users.remove(rid1);

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(session), 1);
    if (!(session.isRemote())) {
      try (var keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
      }
    }

    session.close();
    session = acquireSession();

    document = session.load(rid);

    session.begin();
    document = session.bindToSession(document);
    users = document.field("users");
    users.remove(rid2);
    Assert.assertTrue(users.isEmpty());

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndex");

    Assert.assertEquals(index.getInternal().size(session), 1);
    if (!(session.isRemote())) {
      try (var keyStreamAsc = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStreamAsc.iterator().next(), new CompositeKey((byte) 1, null, 12L, 14L, 12));
      }
    }

    session.close();
    session = acquireSession();

    session.begin();
    document = session.load(rid);
    users = document.field("users");
    users.add(rid3);

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndex");

    Assert.assertEquals(index.getInternal().size(session), 1);
    if (!(session.isRemote())) {
      try (var keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
      }
    }

    session.close();
    session = acquireSession();

    session.begin();
    document = session.bindToSession(document);
    users = document.field("users");
    users.add(rid4);

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(session), 2);

    if (!(session.isRemote())) {
      try (var keyStream = index.getInternal().keyStream()) {
        if (rid3.compareTo(rid4) < 0) {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid4, 12L, 14L, 12));
        }
      }
      try (var descStream = index.getInternal().descStream(session)) {
        if (rid3.compareTo(rid4) < 0) {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid4, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
        }
      }
    }

    session.close();
    session = acquireSession();

    session.begin();
    document = session.bindToSession(document);
    document.removeField("users");

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(session), 1);

    if (!(session.isRemote())) {
      try (var keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, null, 12L, 14L, 12));
      }
    }
  }

  public void testMultikeyWithoutFieldAndNoNullSupport() {
    checkEmbeddedDB();

    // generates stubs for index
    session.begin();
    var doc1 = ((EntityImpl) session.newEntity());

    var doc2 = ((EntityImpl) session.newEntity());

    var doc3 = ((EntityImpl) session.newEntity());

    var doc4 = ((EntityImpl) session.newEntity());

    session.commit();

    final RID rid1 = doc1.getIdentity();
    final RID rid2 = doc2.getIdentity();
    final RID rid3 = doc3.getIdentity();
    final RID rid4 = doc4.getIdentity();

    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("TestMultikeyWithoutFieldNoNullSupport");

    clazz.createProperty(session, "state", PropertyType.BYTE);
    clazz.createProperty(session, "users", PropertyType.LINKSET);
    clazz.createProperty(session, "time", PropertyType.LONG);
    clazz.createProperty(session, "reg", PropertyType.LONG);
    clazz.createProperty(session, "no", PropertyType.INTEGER);

    clazz.createIndex(session,
        "MultikeyWithoutFieldIndexNoNullSupport",
        INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true),
        new String[]{"state", "users", "time", "reg", "no"});

    var document = ((EntityImpl) session.newEntity("TestMultikeyWithoutFieldNoNullSupport"));
    document.field("state", (byte) 1);

    Set<RID> users = new HashSet<>();
    users.add(rid1);
    users.add(rid2);

    document.field("users", users);
    document.field("time", 12L);
    document.field("reg", 14L);
    document.field("no", 12);

    session.begin();

    session.commit();

    var index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(session), 2);

    // we support first and last keys check only for embedded storage
    if (!(session.isRemote())) {
      try (var keyStream = index.getInternal().keyStream()) {
        if (rid1.compareTo(rid2) < 0) {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid1, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
        }
      }
      try (var descStream = index.getInternal().descStream(session)) {
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

    session.close();
    session = acquireSession();

    session.begin();
    document = session.load(rid);
    users = document.field("users");
    users.remove(rid1);

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(session), 1);
    if (!(session.isRemote())) {
      try (var keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, rid2, 12L, 14L, 12));
      }
    }

    session.close();
    session = acquireSession();

    session.begin();
    document = session.load(rid);

    users = document.field("users");
    users.remove(rid2);
    Assert.assertTrue(users.isEmpty());

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(session), 0);

    session.close();
    session = acquireSession();
    session.begin();

    document = session.load(rid);
    users = document.field("users");
    users.add(rid3);

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(session), 1);

    if (!(session.isRemote())) {
      try (var keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
      }
    }

    session.close();
    session = acquireSession();

    session.begin();

    document = session.bindToSession(document);
    users = document.field("users");
    users.add(rid4);

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(session), 2);

    if (!(session.isRemote())) {
      try (var keyStream = index.getInternal().keyStream()) {
        if (rid3.compareTo(rid4) < 0) {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              keyStream.iterator().next(), new CompositeKey((byte) 1, rid4, 12L, 14L, 12));
        }
      }
      try (var descStream = index.getInternal().descStream(session)) {
        if (rid3.compareTo(rid4) < 0) {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid4, 12L, 14L, 12));
        } else {
          Assert.assertEquals(
              descStream.iterator().next().first, new CompositeKey((byte) 1, rid3, 12L, 14L, 12));
        }
      }
    }

    session.close();
    session = acquireSession();

    session.begin();
    document = session.bindToSession(document);
    document.removeField("users");

    session.commit();

    index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(session), 0);
  }

  public void testNullValuesCountSBTreeUnique() {
    checkEmbeddedDB();

    var nullSBTreeClass = session.getSchema().createClass("NullValuesCountSBTreeUnique");
    nullSBTreeClass.createProperty(session, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(session, "NullValuesCountSBTreeUniqueIndex", INDEX_TYPE.UNIQUE,
        "field");

    session.begin();
    var docOne = ((EntityImpl) session.newEntity("NullValuesCountSBTreeUnique"));
    docOne.field("field", 1);

    var docTwo = ((EntityImpl) session.newEntity("NullValuesCountSBTreeUnique"));
    docTwo.field("field", null);

    session.commit();

    var index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "NullValuesCountSBTreeUniqueIndex");
    Assert.assertEquals(index.getInternal().size(session), 2);
    try (var stream = index.getInternal().stream(session)) {
      try (var nullStream = index.getInternal().getRids(session, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountSBTreeNotUniqueOne() {
    checkEmbeddedDB();

    var nullSBTreeClass =
        session.getMetadata().getSchema().createClass("NullValuesCountSBTreeNotUniqueOne");
    nullSBTreeClass.createProperty(session, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(session,
        "NullValuesCountSBTreeNotUniqueOneIndex", INDEX_TYPE.NOTUNIQUE, "field");

    session.begin();
    var docOne = ((EntityImpl) session.newEntity("NullValuesCountSBTreeNotUniqueOne"));
    docOne.field("field", 1);

    var docTwo = ((EntityImpl) session.newEntity("NullValuesCountSBTreeNotUniqueOne"));
    docTwo.field("field", null);

    session.commit();

    var index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "NullValuesCountSBTreeNotUniqueOneIndex");
    Assert.assertEquals(index.getInternal().size(session), 2);
    try (var stream = index.getInternal().stream(session)) {
      try (var nullStream = index.getInternal().getRids(session, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountSBTreeNotUniqueTwo() {
    checkEmbeddedDB();

    var nullSBTreeClass =
        session.getMetadata().getSchema().createClass("NullValuesCountSBTreeNotUniqueTwo");
    nullSBTreeClass.createProperty(session, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(session,
        "NullValuesCountSBTreeNotUniqueTwoIndex", INDEX_TYPE.NOTUNIQUE, "field");

    session.begin();
    var docOne = ((EntityImpl) session.newEntity("NullValuesCountSBTreeNotUniqueTwo"));
    docOne.field("field", null);

    var docTwo = ((EntityImpl) session.newEntity("NullValuesCountSBTreeNotUniqueTwo"));
    docTwo.field("field", null);

    session.commit();

    var index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "NullValuesCountSBTreeNotUniqueTwoIndex");
    try (var stream = index.getInternal().stream(session)) {
      try (var nullStream = index.getInternal().getRids(session, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count()
                + nullStream.findAny().map(v -> 1).orElse(0),
            1);
      }
    }
    Assert.assertEquals(index.getInternal().size(session), 2);
  }

  public void testNullValuesCountHashUnique() {
    checkEmbeddedDB();
    var nullSBTreeClass = session.getSchema().createClass("NullValuesCountHashUnique");
    nullSBTreeClass.createProperty(session, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(session,
        "NullValuesCountHashUniqueIndex", INDEX_TYPE.UNIQUE, "field");

    session.begin();
    var docOne = ((EntityImpl) session.newEntity("NullValuesCountHashUnique"));
    docOne.field("field", 1);

    var docTwo = ((EntityImpl) session.newEntity("NullValuesCountHashUnique"));
    docTwo.field("field", null);

    session.commit();

    var index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "NullValuesCountHashUniqueIndex");
    Assert.assertEquals(index.getInternal().size(session), 2);
    try (var stream = index.getInternal().stream(session)) {
      try (var nullStream = index.getInternal().getRids(session, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountHashNotUniqueOne() {
    checkEmbeddedDB();

    var nullSBTreeClass = session.getSchema()
        .createClass("NullValuesCountHashNotUniqueOne");
    nullSBTreeClass.createProperty(session, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(session,
        "NullValuesCountHashNotUniqueOneIndex", INDEX_TYPE.NOTUNIQUE, "field");

    session.begin();
    var docOne = ((EntityImpl) session.newEntity("NullValuesCountHashNotUniqueOne"));
    docOne.field("field", 1);

    var docTwo = ((EntityImpl) session.newEntity("NullValuesCountHashNotUniqueOne"));
    docTwo.field("field", null);

    session.commit();

    var index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "NullValuesCountHashNotUniqueOneIndex");
    Assert.assertEquals(index.getInternal().size(session), 2);
    try (var stream = index.getInternal().stream(session)) {
      try (var nullStream = index.getInternal().getRids(session, null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountHashNotUniqueTwo() {
    checkEmbeddedDB();

    var nullSBTreeClass =
        session.getMetadata().getSchema().createClass("NullValuesCountHashNotUniqueTwo");
    nullSBTreeClass.createProperty(session, "field", PropertyType.INTEGER);
    nullSBTreeClass.createIndex(session,
        "NullValuesCountHashNotUniqueTwoIndex", INDEX_TYPE.NOTUNIQUE, "field");

    session.begin();
    var docOne = ((EntityImpl) session.newEntity("NullValuesCountHashNotUniqueTwo"));
    docOne.field("field", null);

    var docTwo = ((EntityImpl) session.newEntity("NullValuesCountHashNotUniqueTwo"));
    docTwo.field("field", null);

    session.commit();

    var index =
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(session, "NullValuesCountHashNotUniqueTwoIndex");
    try (var stream = index.getInternal().stream(session)) {
      try (var nullStream = index.getInternal().getRids(session, null)) {
        Assert.assertEquals(
            stream.map(pair -> pair.first).distinct().count()
                + nullStream.findAny().map(v -> 1).orElse(0),
            1);
      }
    }
    Assert.assertEquals(index.getInternal().size(session), 2);
  }

  @Test
  public void testParamsOrder() {
    session.command("CREATE CLASS Task extends V").close();
    session
        .command("CREATE PROPERTY Task.projectId STRING (MANDATORY TRUE, NOTNULL, MAX 20)")
        .close();
    session.command("CREATE PROPERTY Task.seq SHORT ( MANDATORY TRUE, NOTNULL, MIN 0)").close();
    session.command("CREATE INDEX TaskPK ON Task (projectId, seq) UNIQUE").close();

    session.begin();
    session.command("INSERT INTO Task (projectId, seq) values ( 'foo', 2)").close();
    session.command("INSERT INTO Task (projectId, seq) values ( 'bar', 3)").close();
    session.commit();

    var results =
        session
            .query("select from Task where projectId = 'foo' and seq = 2")
            .vertexStream()
            .toList();
    Assert.assertEquals(results.size(), 1);
  }

  private void assertIndexUsage(ResultSet resultSet) {
    var executionPlan = resultSet.getExecutionPlan();
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
