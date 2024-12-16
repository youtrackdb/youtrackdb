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

import com.jetbrains.youtrack.db.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes;
import com.jetbrains.youtrack.db.internal.core.tx.RollbackException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class TransactionOptimisticTest extends BaseDBTest {

  @Parameters(value = "remote")
  public TransactionOptimisticTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testTransactionOptimisticRollback() {
    if (database.getClusterIdByName("binary") == -1) {
      database.addBlobCluster("binary");
    }

    long rec = database.countClusterElements("binary");

    database.begin();

    Blob recordBytes = new RecordBytes("This is the first version".getBytes());
    ((RecordAbstract) recordBytes).save("binary");

    database.rollback();

    Assert.assertEquals(database.countClusterElements("binary"), rec);
  }

  @Test(dependsOnMethods = "testTransactionOptimisticRollback")
  public void testTransactionOptimisticCommit() {
    if (database.getClusterIdByName("binary") == -1) {
      database.addBlobCluster("binary");
    }

    long tot = database.countClusterElements("binary");

    database.begin();

    Blob recordBytes = new RecordBytes("This is the first version".getBytes());
    ((RecordAbstract) recordBytes).save("binary");

    database.commit();

    Assert.assertEquals(database.countClusterElements("binary"), tot + 1);
  }

  @Test(dependsOnMethods = "testTransactionOptimisticCommit")
  public void testTransactionOptimisticConcurrentException() {
    if (database.getClusterIdByName("binary") == -1) {
      database.addBlobCluster("binary");
    }

    DatabaseSessionInternal db2 = acquireSession();
    database.activateOnCurrentThread();
    Blob record1 = new RecordBytes("This is the first version".getBytes());

    database.begin();
    ((RecordAbstract) record1).save("binary");
    database.commit();

    try {
      database.begin();

      // RE-READ THE RECORD
      record1 = database.load(record1.getIdentity());

      DatabaseRecordThreadLocal.instance().set(db2);
      Blob record2 = db2.load(record1.getIdentity());
      RecordInternal.fill(
          record2,
          record2.getIdentity(),
          record2.getVersion(),
          "This is the second version".getBytes(),
          true);
      db2.begin();
      record2.save();
      db2.commit();

      DatabaseRecordThreadLocal.instance().set(database);
      RecordInternal.fill(
          record1,
          record1.getIdentity(),
          record1.getVersion(),
          "This is the third version".getBytes(),
          true);
      record1.save();

      database.commit();

      Assert.fail();

    } catch (ConcurrentModificationException e) {
      Assert.assertTrue(true);
      database.rollback();

    } finally {
      database.close();

      db2.activateOnCurrentThread();
      db2.close();
    }
  }

  @Test(dependsOnMethods = "testTransactionOptimisticConcurrentException")
  public void testTransactionOptimisticCacheMgmt1Db() throws IOException {
    if (database.getClusterIdByName("binary") == -1) {
      database.addBlobCluster("binary");
    }

    Blob record = new RecordBytes("This is the first version".getBytes());
    database.begin();
    record.save();
    database.commit();

    try {
      database.begin();

      // RE-READ THE RECORD
      record = database.load(record.getIdentity());
      int v1 = record.getVersion();
      RecordInternal.fill(
          record, record.getIdentity(), v1, "This is the second version".getBytes(), true);
      record.save();
      database.commit();

      record = database.bindToSession(record);
      Assert.assertEquals(record.getVersion(), v1 + 1);
      Assert.assertTrue(new String(record.toStream()).contains("second"));
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testTransactionOptimisticCacheMgmt1Db")
  public void testTransactionOptimisticCacheMgmt2Db() throws IOException {
    if (database.getClusterIdByName("binary") == -1) {
      database.addBlobCluster("binary");
    }

    DatabaseSessionInternal db2 = acquireSession();
    Blob record1 = new RecordBytes("This is the first version".getBytes());
    db2.begin();
    record1.save();
    db2.commit();

    try {
      DatabaseRecordThreadLocal.instance().set(database);
      database.begin();

      // RE-READ THE RECORD
      record1 = database.load(record1.getIdentity());
      int v1 = record1.getVersion();
      RecordInternal.fill(
          record1, record1.getIdentity(), v1, "This is the second version".getBytes(), true);
      record1.save();

      database.commit();

      db2.activateOnCurrentThread();

      Blob record2 = db2.load(record1.getIdentity());
      Assert.assertEquals(record2.getVersion(), v1 + 1);
      Assert.assertTrue(new String(record2.toStream()).contains("second"));

    } finally {

      database.activateOnCurrentThread();
      database.close();

      db2.activateOnCurrentThread();
      db2.close();
    }
  }

  @Test(dependsOnMethods = "testTransactionOptimisticCacheMgmt2Db")
  public void testTransactionMultipleRecords() throws IOException {
    final Schema schema = database.getMetadata().getSchema();

    if (!schema.existsClass("Account")) {
      schema.createClass("Account");
    }

    long totalAccounts = database.countClass("Account");

    String json =
        "{ \"@class\": \"Account\", \"type\": \"Residence\", \"street\": \"Piazza di Spagna\"}";

    database.begin();
    for (int g = 0; g < 1000; g++) {
      EntityImpl doc = new EntityImpl("Account");
      doc.fromJSON(json);
      doc.field("nr", g);

      doc.save();
    }
    database.commit();

    Assert.assertEquals(database.countClass("Account"), totalAccounts + 1000);

    database.close();
  }

  @SuppressWarnings("unchecked")
  public void createGraphInTx() {
    final Schema schema = database.getMetadata().getSchema();

    if (!schema.existsClass("Profile")) {
      schema.createClass("Profile");
    }

    database.begin();

    EntityImpl kim = new EntityImpl("Profile").field("name", "Kim").field("surname", "Bauer");
    EntityImpl teri = new EntityImpl("Profile").field("name", "Teri").field("surname", "Bauer");
    EntityImpl jack = new EntityImpl("Profile").field("name", "Jack").field("surname", "Bauer");

    ((HashSet<EntityImpl>) jack.field("following", new HashSet<EntityImpl>())
        .field("following"))
        .add(kim);
    ((HashSet<EntityImpl>) kim.field("following", new HashSet<EntityImpl>()).field("following"))
        .add(teri);
    ((HashSet<EntityImpl>) teri.field("following", new HashSet<EntityImpl>())
        .field("following"))
        .add(jack);

    jack.save();

    database.commit();

    database.close();
    database = acquireSession();

    EntityImpl loadedJack = database.load(jack.getIdentity());
    Assert.assertEquals(loadedJack.field("name"), "Jack");
    Collection<Identifiable> jackFollowings = loadedJack.field("following");
    Assert.assertNotNull(jackFollowings);
    Assert.assertEquals(jackFollowings.size(), 1);

    var loadedKim = jackFollowings.iterator().next().getEntity();
    Assert.assertEquals(loadedKim.getProperty("name"), "Kim");
    Collection<Identifiable> kimFollowings = loadedKim.getProperty("following");
    Assert.assertNotNull(kimFollowings);
    Assert.assertEquals(kimFollowings.size(), 1);

    var loadedTeri = kimFollowings.iterator().next().getEntity();
    Assert.assertEquals(loadedTeri.getProperty("name"), "Teri");
    Collection<Identifiable> teriFollowings = loadedTeri.getProperty("following");
    Assert.assertNotNull(teriFollowings);
    Assert.assertEquals(teriFollowings.size(), 1);

    Assert.assertEquals(teriFollowings.iterator().next().getEntity().getProperty("name"), "Jack");

    database.close();
  }

  public void testNestedTx() throws Exception {
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    final Callable<Void> assertEmptyRecord =
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            final DatabaseSessionInternal db = acquireSession();
            try {
              Assert.assertEquals(db.countClass("NestedTxClass"), 0);
            } finally {
              db.close();
            }

            return null;
          }
        };

    final Schema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("NestedTxClass")) {
      schema.createClass("NestedTxClass");
    }

    database.begin();

    final EntityImpl externalDocOne = new EntityImpl("NestedTxClass");
    externalDocOne.field("v", "val1");
    externalDocOne.save();

    Future assertFuture = executorService.submit(assertEmptyRecord);
    assertFuture.get();

    database.begin();

    final EntityImpl externalDocTwo = new EntityImpl("NestedTxClass");
    externalDocTwo.field("v", "val2");
    externalDocTwo.save();

    assertFuture = executorService.submit(assertEmptyRecord);
    assertFuture.get();

    database.commit();

    assertFuture = executorService.submit(assertEmptyRecord);
    assertFuture.get();

    final EntityImpl externalDocThree = new EntityImpl("NestedTxClass");
    externalDocThree.field("v", "val3");
    externalDocThree.save();

    database.commit();

    Assert.assertFalse(database.getTransaction().isActive());
    Assert.assertEquals(database.countClass("NestedTxClass"), 3);
  }

  public void testNestedTxRollbackOne() throws Exception {
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    final Callable<Void> assertEmptyRecord =
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            final DatabaseSessionInternal db = acquireSession();
            try {
              Assert.assertEquals(db.countClass("NestedTxRollbackOne"), 1);
            } finally {
              db.close();
            }

            return null;
          }
        };

    final Schema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("NestedTxRollbackOne")) {
      schema.createClass("NestedTxRollbackOne");
    }

    EntityImpl brokenDocOne = new EntityImpl("NestedTxRollbackOne");
    database.begin();
    brokenDocOne.save();
    database.commit();
    try {
      database.begin();

      final EntityImpl externalDocOne = new EntityImpl("NestedTxRollbackOne");
      externalDocOne.field("v", "val1");
      externalDocOne.save();

      Future assertFuture = executorService.submit(assertEmptyRecord);
      assertFuture.get();

      database.begin();
      EntityImpl externalDocTwo = new EntityImpl("NestedTxRollbackOne");
      externalDocTwo.field("v", "val2");
      externalDocTwo.save();

      assertFuture = executorService.submit(assertEmptyRecord);
      assertFuture.get();

      brokenDocOne = database.bindToSession(brokenDocOne);
      brokenDocOne.setDirty();
      brokenDocOne.save();

      database.commit();

      assertFuture = executorService.submit(assertEmptyRecord);
      assertFuture.get();

      final EntityImpl externalDocThree = new EntityImpl("NestedTxRollbackOne");
      externalDocThree.field("v", "val3");

      database.begin();
      externalDocThree.save();
      database.commit();

      var brokenRid = brokenDocOne.getIdentity();
      executorService
          .submit(
              () -> {
                try (DatabaseSessionInternal db = acquireSession()) {
                  db.executeInTx(() -> {
                    EntityImpl brokenDocTwo = db.load(brokenRid);
                    brokenDocTwo.field("v", "vstr");

                    brokenDocTwo.save();
                  });
                }
              }).get();

      database.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
      database.rollback();
    }

    Assert.assertFalse(database.getTransaction().isActive());
    Assert.assertEquals(database.countClass("NestedTxRollbackOne"), 1);
  }

  public void testNestedTxRollbackTwo() {
    final Schema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("NestedTxRollbackTwo")) {
      schema.createClass("NestedTxRollbackTwo");
    }

    database.begin();
    try {
      final EntityImpl externalDocOne = new EntityImpl("NestedTxRollbackTwo");
      externalDocOne.field("v", "val1");
      externalDocOne.save();

      database.begin();

      final EntityImpl externalDocTwo = new EntityImpl("NestedTxRollbackTwo");
      externalDocTwo.field("v", "val2");
      externalDocTwo.save();

      database.rollback();

      database.begin();
      Assert.fail();
    } catch (RollbackException e) {
      database.rollback();
    }

    Assert.assertFalse(database.getTransaction().isActive());
    Assert.assertEquals(database.countClass("NestedTxRollbackTwo"), 0);
  }
}
