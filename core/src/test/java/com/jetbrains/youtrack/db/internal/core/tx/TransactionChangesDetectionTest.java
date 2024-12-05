package com.jetbrains.youtrack.db.internal.core.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TransactionChangesDetectionTest {

  private YouTrackDB factory;
  private YTDatabaseSessionInternal database;

  @Before
  public void before() {
    factory =
        OCreateDatabaseUtil.createDatabase(
            TransactionChangesDetectionTest.class.getSimpleName(),
            DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    database =
        (YTDatabaseSessionInternal)
            factory.open(
                TransactionChangesDetectionTest.class.getSimpleName(),
                "admin",
                OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    database.createClass("test");
  }

  @After
  public void after() {
    database.close();
    factory.drop(TransactionChangesDetectionTest.class.getSimpleName());
    factory.close();
  }

  @Test
  public void testTransactionChangeTrackingCompleted() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    database.save(new EntityImpl("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isStartedOnServer());
    assertEquals(1, currentTx.getEntryCount());
    assertEquals(OTransaction.TXSTATUS.BEGUN, currentTx.getStatus());

    currentTx.resetChangesTracking();
    database.save(new EntityImpl("test"));
    assertTrue(currentTx.isChanged());
    assertTrue(currentTx.isStartedOnServer());
    assertEquals(2, currentTx.getEntryCount());
    assertEquals(OTransaction.TXSTATUS.BEGUN, currentTx.getStatus());
    database.commit();
    assertEquals(OTransaction.TXSTATUS.COMPLETED, currentTx.getStatus());
  }

  @Test
  public void testTransactionChangeTrackingRolledBack() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    database.save(new EntityImpl("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isStartedOnServer());
    assertEquals(1, currentTx.getEntryCount());
    assertEquals(OTransaction.TXSTATUS.BEGUN, currentTx.getStatus());
    database.rollback();
    assertEquals(OTransaction.TXSTATUS.ROLLED_BACK, currentTx.getStatus());
  }

  @Test
  public void testTransactionChangeTrackingAfterRollback() {
    database.begin();
    final OTransactionOptimistic initialTx = (OTransactionOptimistic) database.getTransaction();
    database.save(new EntityImpl("test"));
    assertEquals(1, initialTx.getTxStartCounter());
    database.rollback();
    assertEquals(OTransaction.TXSTATUS.ROLLED_BACK, initialTx.getStatus());
    assertEquals(0, initialTx.getEntryCount());

    database.begin();
    assertTrue(database.getTransaction() instanceof OTransactionOptimistic);
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    assertEquals(1, currentTx.getTxStartCounter());
    database.save(new EntityImpl("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isStartedOnServer());
    assertEquals(1, currentTx.getEntryCount());
    assertEquals(OTransaction.TXSTATUS.BEGUN, currentTx.getStatus());
  }

  @Test
  public void testTransactionTxStartCounterCommits() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    database.save(new EntityImpl("test"));
    assertEquals(1, currentTx.getTxStartCounter());
    assertEquals(1, currentTx.getEntryCount());

    database.begin();
    assertEquals(2, currentTx.getTxStartCounter());
    database.commit();
    assertEquals(1, currentTx.getTxStartCounter());
    database.save(new EntityImpl("test"));
    database.commit();
    assertEquals(0, currentTx.getTxStartCounter());
  }

  @Test(expected = YTRollbackException.class)
  public void testTransactionRollbackCommit() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    assertEquals(1, currentTx.getTxStartCounter());
    database.begin();
    assertEquals(2, currentTx.getTxStartCounter());
    database.rollback();
    assertEquals(1, currentTx.getTxStartCounter());
    database.commit();
    fail("Should throw an 'YTRollbackException'.");
  }

  @Test
  public void testTransactionTwoStartedThreeCompleted() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    assertEquals(1, currentTx.getTxStartCounter());
    database.begin();
    assertEquals(2, currentTx.getTxStartCounter());
    database.commit();
    assertEquals(1, currentTx.getTxStartCounter());
    database.commit();
    assertEquals(0, currentTx.getTxStartCounter());
    assertFalse(currentTx.isActive());
  }
}
