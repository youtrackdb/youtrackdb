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
    var db1 = acquireSession();
    var db2 = acquireSession();



    db2.begin();
    var record1 = ((EntityImpl) db2.newEntity());
    record1
        .field("value", "This is the first version");

    db2.commit();

    // RE-READ THE RECORD
    db2.activateOnCurrentThread();
    db2.begin();
    EntityImpl record2 = db2.load(record1.getIdentity());

    record2.field("value", "This is the second version");

    db2.commit();

    db2.begin();
    record2 = db2.bindToSession(record2);
    record2.field("value", "This is the third version");

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

    var doc = ((EntityImpl) session.newEntity("Account"));
    session.begin();
    doc.field("version", 0);

    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    doc.setDirty();
    doc.field("testmvcc", true);
    RecordInternal.setVersion(doc, doc.getVersion() + 1);
    try {

      session.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testTransactionPreListenerRollback() throws IOException {
    session.begin();
    var record1 = ((EntityImpl) session.newEntity());

    record1
        .field("value", "This is the first version");

    session.commit();

    final var listener =
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

    session.registerListener(listener);
    session.begin();

    try {
      session.commit();
      Assert.fail();
    } catch (TransactionException e) {
      Assert.assertTrue(true);
    } finally {
      session.unregisterListener(listener);
    }
  }

  @Test
  public void testTransactionWithDuplicateUniqueIndexValues() {
    var fruitClass = session.getMetadata().getSchema().getClass("Fruit");

    if (fruitClass == null) {
      fruitClass = session.getMetadata().getSchema().createClass("Fruit");

      fruitClass.createProperty(session, "name", PropertyType.STRING);
      fruitClass.createProperty(session, "color", PropertyType.STRING);

      session
          .getMetadata()
          .getSchema()
          .getClass("Fruit")
          .getProperty(session, "color")
          .createIndex(session, SchemaClass.INDEX_TYPE.UNIQUE);
    }

    Assert.assertEquals(session.countClusterElements("Fruit"), 0);

    try {
      session.begin();

      var apple = ((EntityImpl) session.newEntity("Fruit")).field("name", "Apple")
          .field("color", "Red");
      var orange = ((EntityImpl) session.newEntity("Fruit")).field("name", "Orange")
          .field("color", "Orange");
      var banana = ((EntityImpl) session.newEntity("Fruit")).field("name", "Banana")
          .field("color", "Yellow");
      var kumquat = ((EntityImpl) session.newEntity("Fruit")).field("name", "Kumquat")
          .field("color", "Orange");

      session.commit();

      Assert.assertEquals(apple.getIdentity().getClusterId(), fruitClass.getClusterIds(session)[0]);
      Assert.assertEquals(orange.getIdentity().getClusterId(),
          fruitClass.getClusterIds(session)[0]);
      Assert.assertEquals(banana.getIdentity().getClusterId(),
          fruitClass.getClusterIds(session)[0]);
      Assert.assertEquals(kumquat.getIdentity().getClusterId(),
          fruitClass.getClusterIds(session)[0]);

      Assert.fail();

    } catch (RecordDuplicatedException e) {
      Assert.assertTrue(true);
      session.rollback();
    }

    Assert.assertEquals(session.countClusterElements("Fruit"), 0);
  }

  @Test
  public void testTransactionalSQL() {
    var prev = session.countClass("Account");

    session.begin();
    session
        .command(new CommandSQL("transactional insert into Account set name = 'txTest1'"))
        .execute(session);
    session.commit();

    Assert.assertEquals(session.countClass("Account"), prev + 1);
  }

  @Test
  public void testTransactionalSQLJoinTx() {
    var prev = session.countClass("Account");

    session.begin();
    session
        .command(new CommandSQL("transactional insert into Account set name = 'txTest2'"))
        .execute(session);

    Assert.assertTrue(session.getTransaction().isActive());

    if (!remoteDB) {
      Assert.assertEquals(session.countClass("Account"), prev + 1);
    }

    session.commit();

    Assert.assertFalse(session.getTransaction().isActive());
    Assert.assertEquals(session.countClass("Account"), prev + 1);
  }
}
