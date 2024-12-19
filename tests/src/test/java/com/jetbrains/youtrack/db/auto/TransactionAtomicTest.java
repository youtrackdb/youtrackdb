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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrack.db.api.exception.TransactionException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.session.SessionListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class TransactionAtomicTest extends BaseDBTest {

  @Parameters(value = "remote")
  public TransactionAtomicTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testTransactionAtomic() {
    DatabaseSessionInternal db1 = acquireSession();
    DatabaseSessionInternal db2 = acquireSession();



    db2.begin();
    EntityImpl record1 = ((EntityImpl) db2.newEntity());
    record1
        .field("value", "This is the first version")
        .save();
    db2.commit();

    // RE-READ THE RECORD
    db2.activateOnCurrentThread();
    db2.begin();
    EntityImpl record2 = db2.load(record1.getIdentity());

    record2.field("value", "This is the second version").save();
    db2.commit();

    db2.begin();
    record2 = db2.bindToSession(record2);
    record2.field("value", "This is the third version").save();
    db2.commit();

    db1.activateOnCurrentThread();
    record1 = db1.bindToSession(record1);
    Assert.assertEquals(record1.field("value"), "This is the third version");
    db1.close();

    db2.activateOnCurrentThread();
    db2.close();

    db1.activateOnCurrentThread();
  }

  @Test
  public void testMVCC() throws IOException {

    EntityImpl doc = ((EntityImpl) db.newEntity("Account"));
    db.begin();
    doc.field("version", 0);
    doc.save();
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    doc.setDirty();
    doc.field("testmvcc", true);
    RecordInternal.setVersion(doc, doc.getVersion() + 1);
    try {
      doc.save();
      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testTransactionPreListenerRollback() throws IOException {
    db.begin();
    EntityImpl record1 = ((EntityImpl) db.newEntity());

    record1
        .field("value", "This is the first version")
        .save();
    db.commit();

    final SessionListener listener =
        new SessionListener() {

          @Override
          public void onAfterTxCommit(DatabaseSession iDatabase) {
          }

          @Override
          public void onAfterTxRollback(DatabaseSession iDatabase) {
          }

          @Override
          public void onBeforeTxBegin(DatabaseSession iDatabase) {
          }

          @Override
          public void onBeforeTxCommit(DatabaseSession iDatabase) {
            throw new RuntimeException("Rollback test");
          }

          @Override
          public void onBeforeTxRollback(DatabaseSession iDatabase) {
          }

          @Override
          public void onClose(DatabaseSession iDatabase) {
          }
        };

    db.registerListener(listener);
    db.begin();

    try {
      db.commit();
      Assert.fail();
    } catch (TransactionException e) {
      Assert.assertTrue(true);
    } finally {
      db.unregisterListener(listener);
    }
  }

  @Test
  public void testTransactionWithDuplicateUniqueIndexValues() {
    SchemaClass fruitClass = db.getMetadata().getSchema().getClass("Fruit");

    if (fruitClass == null) {
      fruitClass = db.getMetadata().getSchema().createClass("Fruit");

      fruitClass.createProperty(db, "name", PropertyType.STRING);
      fruitClass.createProperty(db, "color", PropertyType.STRING);

      db
          .getMetadata()
          .getSchema()
          .getClass("Fruit")
          .getProperty("color")
          .createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE);
    }

    Assert.assertEquals(db.countClusterElements("Fruit"), 0);

    try {
      db.begin();

      EntityImpl apple = ((EntityImpl) db.newEntity("Fruit")).field("name", "Apple")
          .field("color", "Red");
      EntityImpl orange = ((EntityImpl) db.newEntity("Fruit")).field("name", "Orange")
          .field("color", "Orange");
      EntityImpl banana = ((EntityImpl) db.newEntity("Fruit")).field("name", "Banana")
          .field("color", "Yellow");
      EntityImpl kumquat = ((EntityImpl) db.newEntity("Fruit")).field("name", "Kumquat")
          .field("color", "Orange");

      apple.save();
      orange.save();
      banana.save();
      kumquat.save();

      db.commit();

      Assert.assertEquals(apple.getIdentity().getClusterId(), fruitClass.getClusterIds()[0]);
      Assert.assertEquals(orange.getIdentity().getClusterId(), fruitClass.getClusterIds()[0]);
      Assert.assertEquals(banana.getIdentity().getClusterId(), fruitClass.getClusterIds()[0]);
      Assert.assertEquals(kumquat.getIdentity().getClusterId(), fruitClass.getClusterIds()[0]);

      Assert.fail();

    } catch (RecordDuplicatedException e) {
      Assert.assertTrue(true);
      db.rollback();
    }

    Assert.assertEquals(db.countClusterElements("Fruit"), 0);
  }

  @Test
  public void testTransactionalSQL() {
    long prev = db.countClass("Account");

    db.begin();
    db
        .command(new CommandSQL("transactional insert into Account set name = 'txTest1'"))
        .execute(db);
    db.commit();

    Assert.assertEquals(db.countClass("Account"), prev + 1);
  }

  @Test
  public void testTransactionalSQLJoinTx() {
    long prev = db.countClass("Account");

    db.begin();
    db
        .command(new CommandSQL("transactional insert into Account set name = 'txTest2'"))
        .execute(db);

    Assert.assertTrue(db.getTransaction().isActive());

    if (!remoteDB) {
      Assert.assertEquals(db.countClass("Account"), prev + 1);
    }

    db.commit();

    Assert.assertFalse(db.getTransaction().isActive());
    Assert.assertEquals(db.countClass("Account"), prev + 1);
  }
}
