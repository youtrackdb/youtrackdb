package com.jetbrains.youtrack.db.internal.core.tx;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.exception.YTConcurrentCreateException;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TransactionRidAllocationTest {

  private YouTrackDB youTrackDB;
  private YTDatabaseSessionInternal db;

  @Before
  public void before() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    db =
        (YTDatabaseSessionInternal)
            youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void testAllocation() {
    db.begin();
    Vertex v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionInternal) db.getTransaction());
    YTRID generated = v.getIdentity();
    assertTrue(generated.isValid());

    var db1 = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    try {
      db1.load(generated);
      Assert.fail();
    } catch (YTRecordNotFoundException e) {
      // ignore
    }

    db1.close();
  }

  @Test
  public void testAllocationCommit() {
    db.begin();
    Vertex v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionOptimistic) db.getTransaction());
    YTRID generated = v.getIdentity();
    ((AbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((OTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertNotNull(db1.load(generated));
    db1.close();
  }

  @Test
  public void testMultipleDbAllocationAndCommit() {
    YTDatabaseSessionInternal second;
    youTrackDB.execute(
        "create database "
            + "secondTest"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    second =
        (YTDatabaseSessionInternal)
            youTrackDB.open("secondTest", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    db.activateOnCurrentThread();
    db.begin();
    Vertex v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionInternal) db.getTransaction());
    YTRID generated = v.getIdentity();
    OTransaction transaction = db.getTransaction();
    List<ORawPair<RecordAbstract, Byte>> recordOperations = new ArrayList<>();
    for (ORecordOperation operation : transaction.getRecordOperations()) {
      var record = operation.record;
      recordOperations.add(new ORawPair<>(record.copy(), operation.type));
    }

    second.activateOnCurrentThread();
    second.begin();
    OTransactionOptimistic transactionOptimistic = (OTransactionOptimistic) second.getTransaction();
    for (var recordOperation : recordOperations) {
      var record = recordOperation.first;
      record.setup(second);
      transactionOptimistic.addRecord(record, recordOperation.second, null);
    }

    ((AbstractPaginatedStorage) second.getStorage()).preallocateRids(transactionOptimistic);
    db.activateOnCurrentThread();
    ((AbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((OTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotNull(db1.load(generated));

    db1.close();
    second.activateOnCurrentThread();
    ((AbstractPaginatedStorage) second.getStorage())
        .commitPreAllocated((OTransactionOptimistic) second.getTransaction());
    second.close();
    var db2 = youTrackDB.open("secondTest", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotNull(db2.load(generated));
    db2.close();
  }

  @Test(expected = YTConcurrentCreateException.class)
  public void testMultipleDbAllocationNotAlignedFailure() {
    YTDatabaseSessionInternal second;
    youTrackDB.execute(
        "create database "
            + "secondTest"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    second =
        (YTDatabaseSessionInternal)
            youTrackDB.open("secondTest", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    // THIS OFFSET FIRST DB FROM THE SECOND
    for (int i = 0; i < 20; i++) {
      second.begin();
      second.save(second.newVertex("V"));
      second.commit();
    }

    db.activateOnCurrentThread();
    db.begin();
    Vertex v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionOptimistic) db.getTransaction());
    OTransaction transaction = db.getTransaction();
    List<ORawPair<RecordAbstract, Byte>> recordOperations = new ArrayList<>();
    for (ORecordOperation operation : transaction.getRecordOperations()) {
      var record = operation.record;
      recordOperations.add(new ORawPair<>(record.copy(), operation.type));
    }

    second.activateOnCurrentThread();
    second.begin();
    OTransactionOptimistic transactionOptimistic = (OTransactionOptimistic) second.getTransaction();
    for (var recordOperation : recordOperations) {
      var record = recordOperation.first;
      record.setup(second);
      transactionOptimistic.addRecord(record, recordOperation.second, null);
    }
    ((AbstractPaginatedStorage) second.getStorage()).preallocateRids(transactionOptimistic);
  }

  @Test
  public void testAllocationMultipleCommit() {
    db.begin();

    List<Record> orecords = new ArrayList<>();
    Vertex v0 = db.newVertex("V");
    db.save(v0);
    for (int i = 0; i < 20; i++) {
      Vertex v = db.newVertex("V");
      Edge edge = v0.addEdge(v);
      orecords.add(db.save(edge));
      orecords.add(db.save(v));
    }

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionInternal) db.getTransaction());
    List<YTRID> allocated = new ArrayList<>();
    for (Record rec : orecords) {
      allocated.add(rec.getIdentity());
    }
    ((AbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((OTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    for (final YTRID id : allocated) {
      assertNotNull(db1.load(id));
    }
    db1.close();
  }

  @After
  public void after() {
    db.activateOnCurrentThread();
    db.close();
    youTrackDB.close();
  }
}
