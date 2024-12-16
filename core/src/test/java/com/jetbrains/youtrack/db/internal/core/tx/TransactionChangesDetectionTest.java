package com.jetbrains.youtrack.db.internal.core.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TransactionChangesDetectionTest {

  private YouTrackDB factory;
  private DatabaseSessionInternal database;

  @Before
  public void before() {
    factory =
        CreateDatabaseUtil.createDatabase(
            TransactionChangesDetectionTest.class.getSimpleName(),
            DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    database =
        (DatabaseSessionInternal)
            factory.open(
                TransactionChangesDetectionTest.class.getSimpleName(),
                "admin",
                CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    final TransactionOptimistic currentTx = (TransactionOptimistic) database.getTransaction();
    database.save(new EntityImpl("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isStartedOnServer());
    assertEquals(1, currentTx.getEntryCount());
    assertEquals(FrontendTransaction.TXSTATUS.BEGUN, currentTx.getStatus());

    currentTx.resetChangesTracking();
    database.save(new EntityImpl("test"));
    assertTrue(currentTx.isChanged());
    assertTrue(currentTx.isStartedOnServer());
    assertEquals(2, currentTx.getEntryCount());
    assertEquals(FrontendTransaction.TXSTATUS.BEGUN, currentTx.getStatus());
    database.commit();
    assertEquals(FrontendTransaction.TXSTATUS.COMPLETED, currentTx.getStatus());
  }

  @Test
  public void testTransactionChangeTrackingRolledBack() {
    database.begin();
    final TransactionOptimistic currentTx = (TransactionOptimistic) database.getTransaction();
    database.save(new EntityImpl("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isStartedOnServer());
    assertEquals(1, currentTx.getEntryCount());
    assertEquals(FrontendTransaction.TXSTATUS.BEGUN, currentTx.getStatus());
    database.rollback();
    assertEquals(FrontendTransaction.TXSTATUS.ROLLED_BACK, currentTx.getStatus());
  }

  @Test
  public void testTransactionChangeTrackingAfterRollback() {
    database.begin();
    final TransactionOptimistic initialTx = (TransactionOptimistic) database.getTransaction();
    database.save(new EntityImpl("test"));
    assertEquals(1, initialTx.getTxStartCounter());
    database.rollback();
    assertEquals(FrontendTransaction.TXSTATUS.ROLLED_BACK, initialTx.getStatus());
    assertEquals(0, initialTx.getEntryCount());

    database.begin();
    assertTrue(database.getTransaction() instanceof TransactionOptimistic);
    final TransactionOptimistic currentTx = (TransactionOptimistic) database.getTransaction();
    assertEquals(1, currentTx.getTxStartCounter());
    database.save(new EntityImpl("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isStartedOnServer());
    assertEquals(1, currentTx.getEntryCount());
    assertEquals(FrontendTransaction.TXSTATUS.BEGUN, currentTx.getStatus());
  }

  @Test
  public void testTransactionTxStartCounterCommits() {
    database.begin();
    final TransactionOptimistic currentTx = (TransactionOptimistic) database.getTransaction();
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

  @Test(expected = RollbackException.class)
  public void testTransactionRollbackCommit() {
    database.begin();
    final TransactionOptimistic currentTx = (TransactionOptimistic) database.getTransaction();
    assertEquals(1, currentTx.getTxStartCounter());
    database.begin();
    assertEquals(2, currentTx.getTxStartCounter());
    database.rollback();
    assertEquals(1, currentTx.getTxStartCounter());
    database.commit();
    fail("Should throw an 'RollbackException'.");
  }

  @Test
  public void testTransactionTwoStartedThreeCompleted() {
    database.begin();
    final TransactionOptimistic currentTx = (TransactionOptimistic) database.getTransaction();
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
