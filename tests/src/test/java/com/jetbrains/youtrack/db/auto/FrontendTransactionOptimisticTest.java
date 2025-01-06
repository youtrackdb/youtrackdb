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
public class FrontendTransactionOptimisticTest extends BaseDBTest {

  @Parameters(value = "remote")
  public FrontendTransactionOptimisticTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testTransactionOptimisticRollback() {
    if (db.getClusterIdByName("binary") == -1) {
      db.addBlobCluster("binary");
    }

    long rec = db.countClusterElements("binary");

    db.begin();

    Blob recordBytes = db.newBlob("This is the first version".getBytes());
    ((RecordAbstract) recordBytes).save("binary");

    db.rollback();

    Assert.assertEquals(db.countClusterElements("binary"), rec);
  }

  @Test(dependsOnMethods = "testTransactionOptimisticRollback")
  public void testTransactionOptimisticCommit() {
    if (db.getClusterIdByName("binary") == -1) {
      db.addBlobCluster("binary");
    }

    long tot = db.countClusterElements("binary");

    db.begin();

    Blob recordBytes = db.newBlob("This is the first version".getBytes());
    ((RecordAbstract) recordBytes).save("binary");

    db.commit();

    Assert.assertEquals(db.countClusterElements("binary"), tot + 1);
  }

  @Test(dependsOnMethods = "testTransactionOptimisticCommit")
  public void testTransactionOptimisticConcurrentException() {
    if (db.getClusterIdByName("binary") == -1) {
      db.addBlobCluster("binary");
    }

    DatabaseSessionInternal db2 = acquireSession();
    db.activateOnCurrentThread();
    Blob record1 = db.newBlob("This is the first version".getBytes());

    db.begin();
    ((RecordAbstract) record1).save("binary");
    db.commit();

    try {
      db.begin();

      // RE-READ THE RECORD
      record1 = db.load(record1.getIdentity());

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

      DatabaseRecordThreadLocal.instance().set(db);
      RecordInternal.fill(
          record1,
          record1.getIdentity(),
          record1.getVersion(),
          "This is the third version".getBytes(),
          true);
      record1.save();

      db.commit();

      Assert.fail();

    } catch (ConcurrentModificationException e) {
      Assert.assertTrue(true);
      db.rollback();

    } finally {
      db.close();

      db2.activateOnCurrentThread();
      db2.close();
    }
  }

  @Test(dependsOnMethods = "testTransactionOptimisticConcurrentException")
  public void testTransactionOptimisticCacheMgmt1Db() throws IOException {
    if (db.getClusterIdByName("binary") == -1) {
      db.addBlobCluster("binary");
    }

    Blob record = db.newBlob("This is the first version".getBytes());
    db.begin();
    record.save();
    db.commit();

    try {
      db.begin();

      // RE-READ THE RECORD
      record = db.load(record.getIdentity());
      int v1 = record.getVersion();
      RecordInternal.fill(
          record, record.getIdentity(), v1, "This is the second version".getBytes(), true);
      record.save();
      db.commit();

      record = db.bindToSession(record);
      Assert.assertEquals(record.getVersion(), v1 + 1);
      Assert.assertTrue(new String(record.toStream()).contains("second"));
    } finally {
      db.close();
    }
  }

  @Test(dependsOnMethods = "testTransactionOptimisticCacheMgmt1Db")
  public void testTransactionOptimisticCacheMgmt2Db() throws IOException {
    if (db.getClusterIdByName("binary") == -1) {
      db.addBlobCluster("binary");
    }

    DatabaseSessionInternal db2 = acquireSession();
    db2.begin();
    Blob record1 = db2.newBlob("This is the first version".getBytes());
    record1.save();
    db2.commit();
    try {
      DatabaseRecordThreadLocal.instance().set(db);
      db.begin();

      // RE-READ THE RECORD
      record1 = db.load(record1.getIdentity());
      int v1 = record1.getVersion();
      RecordInternal.fill(
          record1, record1.getIdentity(), v1, "This is the second version".getBytes(), true);
      record1.save();

      db.commit();

      db2.activateOnCurrentThread();

      Blob record2 = db2.load(record1.getIdentity());
      Assert.assertEquals(record2.getVersion(), v1 + 1);
      Assert.assertTrue(new String(record2.toStream()).contains("second"));

    } finally {

      db.activateOnCurrentThread();
      db.close();

      db2.activateOnCurrentThread();
      db2.close();
    }
  }

  @Test(dependsOnMethods = "testTransactionOptimisticCacheMgmt2Db")
  public void testTransactionMultipleRecords() throws IOException {
    final Schema schema = db.getMetadata().getSchema();

    if (!schema.existsClass("Account")) {
      schema.createClass("Account");
    }

    long totalAccounts = db.countClass("Account");

    String json =
        "{ \"@class\": \"Account\", \"type\": \"Residence\", \"street\": \"Piazza di Spagna\"}";

    db.begin();
    for (int g = 0; g < 1000; g++) {
      EntityImpl doc = ((EntityImpl) db.newEntity("Account"));
      doc.fromJSON(json);
      doc.field("nr", g);

      doc.save();
    }
    db.commit();

    Assert.assertEquals(db.countClass("Account"), totalAccounts + 1000);

    db.close();
  }

  @SuppressWarnings("unchecked")
  public void createGraphInTx() {
    final Schema schema = db.getMetadata().getSchema();

    if (!schema.existsClass("Profile")) {
      schema.createClass("Profile");
    }

    db.begin();

    EntityImpl kim = ((EntityImpl) db.newEntity("Profile")).field("name", "Kim")
        .field("surname", "Bauer");
    EntityImpl teri = ((EntityImpl) db.newEntity("Profile")).field("name", "Teri")
        .field("surname", "Bauer");
    EntityImpl jack = ((EntityImpl) db.newEntity("Profile")).field("name", "Jack")
        .field("surname", "Bauer");

    ((HashSet<EntityImpl>) jack.field("following", new HashSet<EntityImpl>())
        .field("following"))
        .add(kim);
    ((HashSet<EntityImpl>) kim.field("following", new HashSet<EntityImpl>()).field("following"))
        .add(teri);
    ((HashSet<EntityImpl>) teri.field("following", new HashSet<EntityImpl>())
        .field("following"))
        .add(jack);

    jack.save();

    db.commit();

    db.close();
    db = acquireSession();

    EntityImpl loadedJack = db.load(jack.getIdentity());
    Assert.assertEquals(loadedJack.field("name"), "Jack");
    Collection<Identifiable> jackFollowings = loadedJack.field("following");
    Assert.assertNotNull(jackFollowings);
    Assert.assertEquals(jackFollowings.size(), 1);

    var loadedKim = jackFollowings.iterator().next().getEntity(db);
    Assert.assertEquals(loadedKim.getProperty("name"), "Kim");
    Collection<Identifiable> kimFollowings = loadedKim.getProperty("following");
    Assert.assertNotNull(kimFollowings);
    Assert.assertEquals(kimFollowings.size(), 1);

    var loadedTeri = kimFollowings.iterator().next().getEntity(db);
    Assert.assertEquals(loadedTeri.getProperty("name"), "Teri");
    Collection<Identifiable> teriFollowings = loadedTeri.getProperty("following");
    Assert.assertNotNull(teriFollowings);
    Assert.assertEquals(teriFollowings.size(), 1);

    Assert.assertEquals(teriFollowings.iterator().next().getEntity(db).getProperty("name"), "Jack");

    db.close();
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

    final Schema schema = db.getMetadata().getSchema();
    if (!schema.existsClass("NestedTxClass")) {
      schema.createClass("NestedTxClass");
    }

    db.begin();

    final EntityImpl externalDocOne = ((EntityImpl) db.newEntity("NestedTxClass"));
    externalDocOne.field("v", "val1");
    externalDocOne.save();

    Future assertFuture = executorService.submit(assertEmptyRecord);
    assertFuture.get();

    db.begin();

    final EntityImpl externalDocTwo = ((EntityImpl) db.newEntity("NestedTxClass"));
    externalDocTwo.field("v", "val2");
    externalDocTwo.save();

    assertFuture = executorService.submit(assertEmptyRecord);
    assertFuture.get();

    db.commit();

    assertFuture = executorService.submit(assertEmptyRecord);
    assertFuture.get();

    final EntityImpl externalDocThree = ((EntityImpl) db.newEntity("NestedTxClass"));
    externalDocThree.field("v", "val3");
    externalDocThree.save();

    db.commit();

    Assert.assertFalse(db.getTransaction().isActive());
    Assert.assertEquals(db.countClass("NestedTxClass"), 3);
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

    final Schema schema = db.getMetadata().getSchema();
    if (!schema.existsClass("NestedTxRollbackOne")) {
      schema.createClass("NestedTxRollbackOne");
    }

    EntityImpl brokenDocOne = ((EntityImpl) db.newEntity("NestedTxRollbackOne"));
    db.begin();
    brokenDocOne.save();
    db.commit();
    try {
      db.begin();

      final EntityImpl externalDocOne = ((EntityImpl) db.newEntity("NestedTxRollbackOne"));
      externalDocOne.field("v", "val1");
      externalDocOne.save();

      Future assertFuture = executorService.submit(assertEmptyRecord);
      assertFuture.get();

      db.begin();
      EntityImpl externalDocTwo = ((EntityImpl) db.newEntity("NestedTxRollbackOne"));
      externalDocTwo.field("v", "val2");
      externalDocTwo.save();

      assertFuture = executorService.submit(assertEmptyRecord);
      assertFuture.get();

      brokenDocOne = db.bindToSession(brokenDocOne);
      brokenDocOne.setDirty();
      brokenDocOne.save();

      db.commit();

      assertFuture = executorService.submit(assertEmptyRecord);
      assertFuture.get();

      final EntityImpl externalDocThree = ((EntityImpl) db.newEntity("NestedTxRollbackOne"));
      externalDocThree.field("v", "val3");

      db.begin();
      externalDocThree.save();
      db.commit();

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

      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
      db.rollback();
    }

    Assert.assertFalse(db.getTransaction().isActive());
    Assert.assertEquals(db.countClass("NestedTxRollbackOne"), 1);
  }

  public void testNestedTxRollbackTwo() {
    final Schema schema = db.getMetadata().getSchema();
    if (!schema.existsClass("NestedTxRollbackTwo")) {
      schema.createClass("NestedTxRollbackTwo");
    }

    db.begin();
    try {
      final EntityImpl externalDocOne = ((EntityImpl) db.newEntity("NestedTxRollbackTwo"));
      externalDocOne.field("v", "val1");
      externalDocOne.save();

      db.begin();

      final EntityImpl externalDocTwo = ((EntityImpl) db.newEntity("NestedTxRollbackTwo"));
      externalDocTwo.field("v", "val2");
      externalDocTwo.save();

      db.rollback();

      db.begin();
      Assert.fail();
    } catch (RollbackException e) {
      db.rollback();
    }

    Assert.assertFalse(db.getTransaction().isActive());
    Assert.assertEquals(db.countClass("NestedTxRollbackTwo"), 0);
  }
}
