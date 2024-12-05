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

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTConcurrentModificationException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class TransactionConsistencyTest extends DocumentDBBaseTest {

  protected YTDatabaseSessionInternal database1;
  protected YTDatabaseSessionInternal database2;

  public static final String NAME = "name";

  @Parameters(value = "remote")
  public TransactionConsistencyTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void test1RollbackOnConcurrentException() {
    database1 = acquireSession();

    database1.begin();

    // Create docA.
    YTEntityImpl vDocA_db1 = database1.newInstance();
    vDocA_db1.field(NAME, "docA");
    database1.save(vDocA_db1);

    // Create docB.
    YTEntityImpl vDocB_db1 = database1.newInstance();
    vDocB_db1.field(NAME, "docB");

    database1.save(vDocB_db1);
    database1.commit();

    // Keep the IDs.
    YTRID vDocA_Rid = vDocA_db1.getIdentity().copy();
    YTRID vDocB_Rid = vDocB_db1.getIdentity().copy();

    int vDocA_version = -1;
    int vDocB_version = -1;

    database2 = acquireSession();
    database2.begin();
    try {
      // Get docA and update in db2 transaction context
      YTEntityImpl vDocA_db2 = database2.load(vDocA_Rid);
      vDocA_db2.field(NAME, "docA_v2");
      database2.save(vDocA_db2);

      // Concurrent update docA via database1 -> will throw YTConcurrentModificationException at
      // database2.commit().
      database1.activateOnCurrentThread();
      database1.begin();
      try {
        vDocA_db1 = database1.bindToSession(vDocA_db1);

        vDocA_db1.field(NAME, "docA_v3");
        database1.save(vDocA_db1);

        database1.commit();
      } catch (YTConcurrentModificationException e) {
        Assert.fail("Should not failed here...");
      }
      vDocA_db1 = database1.bindToSession(vDocA_db1);
      vDocB_db1 = database1.bindToSession(vDocB_db1);

      Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");
      // Keep the last versions.
      // Following updates should failed and reverted.
      vDocA_version = vDocA_db1.getVersion();
      vDocB_version = vDocB_db1.getVersion();

      // Update docB in db2 transaction context -> should be rollbacked.
      database2.activateOnCurrentThread();
      YTEntityImpl vDocB_db2 = database2.load(vDocB_Rid);
      vDocB_db2.field(NAME, "docB_UpdatedInTranscationThatWillBeRollbacked");
      database2.save(vDocB_db2);

      // Will throw YTConcurrentModificationException
      database2.commit();
      Assert.fail("Should throw YTConcurrentModificationException");
    } catch (YTConcurrentModificationException e) {
      database2.rollback();
    }

    // Force reload all (to be sure it is not a cache problem)
    database1.activateOnCurrentThread();
    database1.close();

    database2.activateOnCurrentThread();
    database2 = acquireSession();

    YTEntityImpl vDocA_db2 = database2.load(vDocA_Rid);
    Assert.assertEquals(vDocA_db2.field(NAME), "docA_v3");
    Assert.assertEquals(vDocA_db2.getVersion(), vDocA_version);

    // docB should be in the first state : "docB"
    YTEntityImpl vDocB_db2 = database2.load(vDocB_Rid);
    Assert.assertEquals(vDocB_db2.field(NAME), "docB");
    Assert.assertEquals(vDocB_db2.getVersion(), vDocB_version);

    database2.close();
  }

  @Test
  public void test4RollbackWithPin() {
    database1 = acquireSession();

    // Create docA.
    YTEntityImpl vDocA_db1 = database1.newInstance();
    vDocA_db1.field(NAME, "docA");
    database1.begin();
    database1.save(vDocA_db1);
    database1.commit();

    // Keep the IDs.
    YTRID vDocA_Rid = vDocA_db1.getIdentity().copy();

    database2 = acquireSession();
    database2.begin();
    try {
      // Get docA and update in db2 transaction context
      YTEntityImpl vDocA_db2 = database2.load(vDocA_Rid);
      vDocA_db2.field(NAME, "docA_v2");
      database2.save(vDocA_db2);

      database1.activateOnCurrentThread();
      database1.begin();
      try {
        vDocA_db1 = database1.bindToSession(vDocA_db1);
        vDocA_db1.field(NAME, "docA_v3");
        database1.save(vDocA_db1);
        database1.commit();
      } catch (YTConcurrentModificationException e) {
        Assert.fail("Should not failed here...");
      }
      vDocA_db1 = database1.bindToSession(vDocA_db1);
      Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");

      // Will throw YTConcurrentModificationException
      database2.activateOnCurrentThread();
      database2.commit();
      Assert.fail("Should throw YTConcurrentModificationException");
    } catch (YTConcurrentModificationException e) {
      database2.rollback();
    }

    // Force reload all (to be sure it is not a cache problem)
    database1.activateOnCurrentThread();
    database1.close();

    database2.activateOnCurrentThread();
    database2.close();
    database2 = acquireSession();

    // docB should be in the last state : "docA_v3"
    YTEntityImpl vDocB_db2 = database2.load(vDocA_Rid);
    Assert.assertEquals(vDocB_db2.field(NAME), "docA_v3");

    database1.activateOnCurrentThread();
    database1.close();

    database2.activateOnCurrentThread();
    database2.close();
  }

  @Test
  public void test3RollbackWithCopyCacheStrategy() {
    database1 = acquireSession();

    // Create docA.
    YTEntityImpl vDocA_db1 = database1.newInstance();
    vDocA_db1.field(NAME, "docA");
    database1.begin();
    database1.save(vDocA_db1);
    database1.commit();

    // Keep the IDs.
    YTRID vDocA_Rid = vDocA_db1.getIdentity().copy();

    database2 = acquireSession();
    database2.begin();
    try {
      // Get docA and update in db2 transaction context
      YTEntityImpl vDocA_db2 = database2.load(vDocA_Rid);
      vDocA_db2.field(NAME, "docA_v2");
      database2.save(vDocA_db2);

      database1.activateOnCurrentThread();
      database1.begin();
      try {
        vDocA_db1 = database1.bindToSession(vDocA_db1);
        vDocA_db1.field(NAME, "docA_v3");
        database1.save(vDocA_db1);
        database1.commit();
      } catch (YTConcurrentModificationException e) {
        Assert.fail("Should not failed here...");
      }

      vDocA_db1 = database1.bindToSession(vDocA_db1);
      Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");

      // Will throw YTConcurrentModificationException
      database2.activateOnCurrentThread();
      database2.commit();
      Assert.fail("Should throw YTConcurrentModificationException");
    } catch (YTConcurrentModificationException e) {
      database2.rollback();
    }

    // Force reload all (to be sure it is not a cache problem)
    database1.activateOnCurrentThread();
    database1.close();

    database2.activateOnCurrentThread();
    database2.close();
    database2 = acquireSession();

    // docB should be in the last state : "docA_v3"
    YTEntityImpl vDocB_db2 = database2.load(vDocA_Rid);
    Assert.assertEquals(vDocB_db2.field(NAME), "docA_v3");

    database1.activateOnCurrentThread();
    database1.close();
    database2.activateOnCurrentThread();
    database2.close();
  }

  @Test
  public void test5CacheUpdatedMultipleDbs() {
    database1 = acquireSession();

    // Create docA in db1
    database1.begin();
    YTEntityImpl vDocA_db1 = database1.newInstance();
    vDocA_db1.field(NAME, "docA");
    database1.save(vDocA_db1);
    database1.commit();

    // Keep the ID.
    YTRID vDocA_Rid = vDocA_db1.getIdentity().copy();

    // Update docA in db2
    database2 = acquireSession();
    database2.begin();
    YTEntityImpl vDocA_db2 = database2.load(vDocA_Rid);
    vDocA_db2.field(NAME, "docA_v2");
    database2.save(vDocA_db2);
    database2.commit();

    // Later... read docA with db1.
    database1.activateOnCurrentThread();
    database1.begin();
    YTEntityImpl vDocA_db1_later = database1.load(vDocA_Rid);
    Assert.assertEquals(vDocA_db1_later.field(NAME), "docA_v2");
    database1.commit();

    database1.close();

    database2.activateOnCurrentThread();
    database2.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void checkVersionsInConnectedDocuments() {
    database = acquireSession();
    database.begin();

    YTEntityImpl kim = new YTEntityImpl("Profile").field("name", "Kim").field("surname", "Bauer");
    YTEntityImpl teri = new YTEntityImpl("Profile").field("name", "Teri").field("surname", "Bauer");
    YTEntityImpl jack = new YTEntityImpl("Profile").field("name", "Jack").field("surname", "Bauer");

    ((HashSet<YTEntityImpl>) jack.field("following", new HashSet<YTEntityImpl>())
        .field("following"))
        .add(kim);
    ((HashSet<YTEntityImpl>) kim.field("following", new HashSet<YTEntityImpl>()).field("following"))
        .add(teri);
    ((HashSet<YTEntityImpl>) teri.field("following", new HashSet<YTEntityImpl>())
        .field("following"))
        .add(jack);

    jack.save();

    database.commit();

    database.close();
    database = acquireSession();

    YTEntityImpl loadedJack = database.load(jack.getIdentity());

    int jackLastVersion = loadedJack.getVersion();
    database.begin();
    loadedJack = database.bindToSession(loadedJack);
    loadedJack.field("occupation", "agent");
    loadedJack.save();
    database.commit();
    Assert.assertTrue(jackLastVersion != database.bindToSession(loadedJack).getVersion());

    loadedJack = database.load(jack.getIdentity());
    Assert.assertTrue(jackLastVersion != database.bindToSession(loadedJack).getVersion());

    database.close();

    database = acquireSession();
    loadedJack = database.load(jack.getIdentity());
    Assert.assertTrue(jackLastVersion != database.bindToSession(loadedJack).getVersion());
    database.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void createLinkInTx() {
    database = createSessionInstance();

    YTClass profile = database.getMetadata().getSchema().createClass("MyProfile", 1);
    YTClass edge = database.getMetadata().getSchema().createClass("MyEdge", 1);
    profile
        .createProperty(database, "name", YTType.STRING)
        .setMin(database, "3")
        .setMax(database, "30")
        .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);
    profile.createProperty(database, "surname", YTType.STRING).setMin(database, "3")
        .setMax(database, "30");
    profile.createProperty(database, "in", YTType.LINKSET, edge);
    profile.createProperty(database, "out", YTType.LINKSET, edge);
    edge.createProperty(database, "in", YTType.LINK, profile);
    edge.createProperty(database, "out", YTType.LINK, profile);

    database.begin();

    YTEntityImpl kim = new YTEntityImpl("MyProfile").field("name", "Kim").field("surname", "Bauer");
    YTEntityImpl teri = new YTEntityImpl("MyProfile").field("name", "Teri")
        .field("surname", "Bauer");
    YTEntityImpl jack = new YTEntityImpl("MyProfile").field("name", "Jack")
        .field("surname", "Bauer");

    YTEntityImpl myedge = new YTEntityImpl("MyEdge").field("in", kim).field("out", jack);
    myedge.save();
    ((HashSet<YTEntityImpl>) kim.field("out", new HashSet<YTRID>()).field("out")).add(myedge);
    ((HashSet<YTEntityImpl>) jack.field("in", new HashSet<YTRID>()).field("in")).add(myedge);

    jack.save();
    kim.save();
    teri.save();
    database.commit();

    YTResultSet result = database.command("select from MyProfile ");

    Assert.assertTrue(result.stream().findAny().isPresent());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void loadRecordTest() {
    database.begin();

    YTEntityImpl kim = new YTEntityImpl("Profile").field("name", "Kim").field("surname", "Bauer");
    YTEntityImpl teri = new YTEntityImpl("Profile").field("name", "Teri").field("surname", "Bauer");
    YTEntityImpl jack = new YTEntityImpl("Profile").field("name", "Jack").field("surname", "Bauer");
    YTEntityImpl chloe = new YTEntityImpl("Profile").field("name", "Chloe")
        .field("surname", "O'Brien");

    ((HashSet<YTEntityImpl>) jack.field("following", new HashSet<YTEntityImpl>())
        .field("following"))
        .add(kim);
    ((HashSet<YTEntityImpl>) kim.field("following", new HashSet<YTEntityImpl>()).field("following"))
        .add(teri);
    ((HashSet<YTEntityImpl>) teri.field("following", new HashSet<YTEntityImpl>())
        .field("following"))
        .add(jack);
    ((HashSet<YTEntityImpl>) teri.field("following")).add(kim);
    ((HashSet<YTEntityImpl>) chloe.field("following", new HashSet<YTEntityImpl>())
        .field("following"))
        .add(jack);
    ((HashSet<YTEntityImpl>) chloe.field("following")).add(teri);
    ((HashSet<YTEntityImpl>) chloe.field("following")).add(kim);

    var schema = database.getSchema();
    var profileClusterIds =
        Arrays.asList(ArrayUtils.toObject(schema.getClass("Profile").getClusterIds()));

    jack.save();
    kim.save();
    teri.save();
    chloe.save();

    database.commit();

    Assert.assertListContainsObject(
        profileClusterIds, jack.getIdentity().getClusterId(), "Cluster id not found");
    Assert.assertListContainsObject(
        profileClusterIds, kim.getIdentity().getClusterId(), "Cluster id not found");
    Assert.assertListContainsObject(
        profileClusterIds, teri.getIdentity().getClusterId(), "Cluster id not found");
    Assert.assertListContainsObject(
        profileClusterIds, chloe.getIdentity().getClusterId(), "Cluster id not found");

    database.load(chloe.getIdentity());
  }

  @Test
  public void testTransactionPopulateDelete() {
    if (!database.getMetadata().getSchema().existsClass("MyFruit")) {
      YTClass fruitClass = database.getMetadata().getSchema().createClass("MyFruit");
      fruitClass.createProperty(database, "name", YTType.STRING);
      fruitClass.createProperty(database, "color", YTType.STRING);
      fruitClass.createProperty(database, "flavor", YTType.STRING);

      database
          .getMetadata()
          .getSchema()
          .getClass("MyFruit")
          .getProperty("name")
          .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);
      database
          .getMetadata()
          .getSchema()
          .getClass("MyFruit")
          .getProperty("color")
          .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);
      database
          .getMetadata()
          .getSchema()
          .getClass("MyFruit")
          .getProperty("flavor")
          .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);
    }

    int chunkSize = 10;
    for (int initialValue = 0; initialValue < 10; initialValue++) {
      Assert.assertEquals(database.countClusterElements("MyFruit"), 0);

      System.out.println(
          "[testTransactionPopulateDelete] Populating chunk "
              + initialValue
              + "... (chunk="
              + chunkSize
              + ")");

      // do insert
      List<YTEntityImpl> v = new ArrayList<>();
      database.begin();
      for (int i = initialValue * chunkSize; i < (initialValue * chunkSize) + chunkSize; i++) {
        YTEntityImpl d =
            new YTEntityImpl("MyFruit")
                .field("name", "" + i)
                .field("color", "FOO")
                .field("flavor", "BAR" + i);
        d.save();
        v.add(d);
      }

      System.out.println(
          "[testTransactionPopulateDelete] Committing chunk " + initialValue + "...");

      database.commit();

      System.out.println(
          "[testTransactionPopulateDelete] Committed chunk "
              + initialValue
              + ", starting to delete all the new entries ("
              + v.size()
              + ")...");

      // do delete
      database.begin();
      for (YTEntityImpl entries : v) {
        database.delete(database.bindToSession(entries));
      }
      database.commit();

      System.out.println("[testTransactionPopulateDelete] Deleted executed successfully");

      Assert.assertEquals(database.countClusterElements("MyFruit"), 0);
    }

    System.out.println("[testTransactionPopulateDelete] End of the test");
  }

  @Test
  public void testConsistencyOnDelete() {
    if (database.getMetadata().getSchema().getClass("Foo") == null) {
      database.createVertexClass("Foo");
    }

    database.begin();
    // Step 1
    // Create several foo's
    var v = database.newVertex("Foo");
    v.setProperty("address", "test1");
    v.save();

    v = database.newVertex("Foo");
    v.setProperty("address", "test2");
    v.save();

    v = database.newVertex("Foo");
    v.setProperty("address", "test3");
    v.save();
    database.commit();

    // remove those foos in a transaction
    // Step 3a
    var result =
        database.query("select * from Foo where address = 'test1'").entityStream().toList();
    Assert.assertEquals(result.size(), 1);
    // Step 4a
    database.begin();
    database.delete(database.bindToSession(result.get(0)));
    database.commit();

    // Step 3b
    result = database.query("select * from Foo where address = 'test2'").entityStream().toList();
    Assert.assertEquals(result.size(), 1);
    // Step 4b
    database.begin();
    database.delete(database.bindToSession(result.get(0)));
    database.commit();

    // Step 3c
    result = database.query("select * from Foo where address = 'test3'").entityStream().toList();
    Assert.assertEquals(result.size(), 1);
    // Step 4c
    database.begin();
    database.delete(database.bindToSession(result.get(0)));
    database.commit();
  }

  @Test
  public void deletesWithinTransactionArentWorking() {
    if (database.getClass("Foo") == null) {
      database.createVertexClass("Foo");
    }
    if (database.getClass("Bar") == null) {
      database.createVertexClass("Bar");
    }
    if (database.getClass("Sees") == null) {
      database.createEdgeClass("Sees");
    }

    // Commenting out the transaction will result in the test succeeding.
    var foo = database.newVertex("Foo");
    foo.setProperty("prop", "test1");
    database.begin();
    foo.save();
    database.commit();

    // Comment out these two lines and the test will succeed. The issue appears to be related to
    // an edge
    // connecting a deleted vertex during a transaction
    var bar = database.newVertex("Bar");
    bar.setProperty("prop", "test1");
    database.begin();
    bar.save();
    database.commit();

    database.begin();
    foo = database.bindToSession(foo);
    bar = database.bindToSession(bar);
    var sees = database.newEdge(foo, bar, "Sees");
    sees.save();
    database.commit();

    var foos = database.query("select * from Foo").stream().toList();
    Assert.assertEquals(foos.size(), 1);

    database.begin();
    database.delete(database.bindToSession(foos.get(0).toEntity()));
    database.commit();
  }

  public void transactionRollbackConstistencyTest() {
    YTClass vertexClass = database.getMetadata().getSchema().createClass("TRVertex");
    YTClass edgeClass = database.getMetadata().getSchema().createClass("TREdge");
    vertexClass.createProperty(database, "in", YTType.LINKSET, edgeClass);
    vertexClass.createProperty(database, "out", YTType.LINKSET, edgeClass);
    edgeClass.createProperty(database, "in", YTType.LINK, vertexClass);
    edgeClass.createProperty(database, "out", YTType.LINK, vertexClass);

    YTClass personClass = database.getMetadata().getSchema().createClass("TRPerson", vertexClass);
    personClass.createProperty(database, "name", YTType.STRING)
        .createIndex(database, YTClass.INDEX_TYPE.UNIQUE);
    personClass.createProperty(database, "surname", YTType.STRING)
        .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);
    personClass.createProperty(database, "version", YTType.INTEGER);

    database.close();

    final int cnt = 4;

    database = createSessionInstance();
    database.begin();
    List<YTEntity> inserted = new ArrayList<>();

    for (int i = 0; i < cnt; i++) {
      YTEntityImpl person = new YTEntityImpl("TRPerson");
      person.field("name", Character.toString((char) ('A' + i)));
      person.field("surname", Character.toString((char) ('A' + (i % 3))));
      person.field("myversion", 0);
      person.field("in", new HashSet<YTEntityImpl>());
      person.field("out", new HashSet<YTEntityImpl>());

      if (i >= 1) {
        YTEntityImpl edge = new YTEntityImpl("TREdge");
        edge.field("in", person.getIdentity());
        edge.field("out", inserted.get(i - 1));
        (person.<Set<YTEntityImpl>>getProperty("out")).add(edge);
        (database.bindToSession(inserted.get(i - 1)).<Set<YTEntityImpl>>getProperty("in")).add(
            edge);
        edge.save();
      }
      inserted.add(person);
      person.save();
    }
    database.commit();

    final YTResultSet result1 = database.command("select from TRPerson");
    Assert.assertEquals(result1.stream().count(), cnt);

    try {
      database.executeInTx(
          () -> {
            List<YTEntity> inserted2 = new ArrayList<>();

            for (int i = 0; i < cnt; i++) {
              YTEntityImpl person = new YTEntityImpl("TRPerson");
              person.field("name", Character.toString((char) ('a' + i)));
              person.field("surname", Character.toString((char) ('a' + (i % 3))));
              person.field("myversion", 0);
              person.field("in", new HashSet<YTEntityImpl>());
              person.field("out", new HashSet<YTEntityImpl>());

              if (i >= 1) {
                YTEntityImpl edge = new YTEntityImpl("TREdge");
                edge.field("in", person.getIdentity());
                edge.field("out", inserted2.get(i - 1));
                (person.<Set<YTEntityImpl>>getProperty("out")).add(edge);
                ((inserted2.get(i - 1)).<Set<YTEntityImpl>>getProperty("in")).add(edge);
                edge.save();
              }

              inserted2.add(person);
              person.save();
            }

            for (int i = 0; i < cnt; i++) {
              if (i != cnt - 1) {
                var doc = database.bindToSession((YTEntityImpl) inserted.get(i));
                doc.setProperty("myversion", 2);
                doc.save();
              }
            }

            var doc = ((YTEntityImpl) inserted.get(cnt - 1));
            database.bindToSession(doc).delete();

            throw new IllegalStateException();
          });
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(true);
    }

    final YTResultSet result2 = database.command("select from TRPerson");
    Assert.assertNotNull(result2);
    Assert.assertEquals(result2.stream().count(), cnt);
  }

  @Test
  public void testQueryIsolation() {
    database.begin();
    var v = database.newVertex();

    v.setProperty("purpose", "testQueryIsolation");
    v.save();

    var result =
        database
            .query("select from V where purpose = 'testQueryIsolation'")
            .entityStream()
            .toList();
    Assert.assertEquals(result.size(), 1);

    database.commit();

    result =
        database
            .query("select from V where purpose = 'testQueryIsolation'")
            .entityStream()
            .toList();
    Assert.assertEquals(result.size(), 1);
  }

  /**
   * When calling .remove(o) on a collection, the row corresponding to o is deleted and not restored
   * when the transaction is rolled back.
   *
   * <p>Commented code after data model change to work around this problem.
   */
  @SuppressWarnings("unused")
  @Test
  public void testRollbackWithRemove() {
    var account = database.newEntity("Account");
    account.setProperty("name", "John Grisham");
    database.begin();
    account = database.save(account);
    database.commit();

    database.begin();
    account = database.bindToSession(account);
    var address1 = database.newEntity("Address");
    address1.setProperty("street", "Mulholland drive");
    address1.save();

    var address2 = database.newEntity("Address");
    address2.setProperty("street", "Via Veneto");

    List<YTEntity> addresses = new ArrayList<>();
    addresses.add(address1);
    addresses.add(address2);

    account.setProperty("addresses", addresses);

    account = database.save(account);
    database.commit();

    database.begin();
    account = database.bindToSession(account);
    String originalName = account.getProperty("name");
    Assert.assertEquals(account.<List<YTIdentifiable>>getProperty("addresses").size(), 2);
    account
        .<List<YTIdentifiable>>getProperty("addresses")
        .remove(1); // delete one of the objects in the Books collection to see how rollback behaves
    Assert.assertEquals(account.<List<YTIdentifiable>>getProperty("addresses").size(), 1);
    account.setProperty(
        "name", "New Name"); // change an attribute to see if the change is rolled back
    account = database.save(account);

    Assert.assertEquals(
        account.<List<YTIdentifiable>>getProperty("addresses").size(),
        1); // before rollback this is fine because one of the books was removed

    database.rollback(); // rollback the transaction

    account = database.bindToSession(account);
    Assert.assertEquals(
        account.<List<YTIdentifiable>>getProperty("addresses").size(),
        2); // this is fine, author still linked to 2 books
    Assert.assertEquals(account.getProperty("name"), originalName); // name is restored

    int bookCount = 0;
    for (var b : database.browseClass("Address")) {
      var street = b.getProperty("street");
      if ("Mulholland drive".equals(street) || "Via Veneto".equals(street)) {
        bookCount++;
      }
    }

    Assert.assertEquals(bookCount, 2); // this fails, only 1 entry in the datastore :(
  }

  public void testTransactionsCache() {
    Assert.assertFalse(database.getTransaction().isActive());
    YTSchema schema = database.getMetadata().getSchema();
    YTClass classA = schema.createClass("TransA");
    classA.createProperty(database, "name", YTType.STRING);
    YTEntityImpl doc = new YTEntityImpl(classA);
    doc.field("name", "test1");

    database.begin();
    doc.save();
    database.commit();
    YTRID orid = doc.getIdentity();
    database.begin();
    Assert.assertTrue(database.getTransaction().isActive());
    doc = orid.getRecord();
    Assert.assertEquals(doc.field("name"), "test1");
    doc.field("name", "test2");
    doc = orid.getRecord();
    Assert.assertEquals(doc.field("name"), "test2");
    // There is NO SAVE!
    database.commit();

    doc = orid.getRecord();
    Assert.assertEquals(doc.field("name"), "test1");
  }
}
