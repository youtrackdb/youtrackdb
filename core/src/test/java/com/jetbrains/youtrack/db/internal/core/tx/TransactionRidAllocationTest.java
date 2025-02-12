package com.jetbrains.youtrack.db.internal.core.tx;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.exception.ConcurrentCreateException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
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
  private DatabaseSessionInternal db;

  @Before
  public void before() {
    youTrackDB =
        CreateDatabaseUtil.createDatabase("test", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    db =
        (DatabaseSessionInternal)
            youTrackDB.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void testAllocation() {
    db.begin();
    var v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((TransactionInternal) db.getTransaction());
    var generated = (RecordId) v.getIdentity();
    assertTrue(generated.isValid());

    var db1 = youTrackDB.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    try {
      db1.load(generated);
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }

    db1.close();
  }

  @Test
  public void testAllocationCommit() {
    db.begin();
    var v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((FrontendTransactionOptimistic) db.getTransaction());
    var generated = v.getIdentity();
    ((AbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((FrontendTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertNotNull(db1.load(generated));
    db1.close();
  }

  @Test
  public void testMultipleDbAllocationAndCommit() {
    DatabaseSessionInternal second;
    youTrackDB.execute(
        "create database "
            + "secondTest"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    second =
        (DatabaseSessionInternal)
            youTrackDB.open("secondTest", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    db.activateOnCurrentThread();
    db.begin();
    var v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((TransactionInternal) db.getTransaction());
    var generated = v.getIdentity();
    var transaction = db.getTransaction();
    List<RawPair<byte[], Byte>> recordOperations = new ArrayList<>();
    for (var operation : transaction.getRecordOperations()) {
      var record = operation.record;
      recordOperations.add(new RawPair<>(record.toStream(), operation.type));
    }

    second.activateOnCurrentThread();
    second.begin();
    var transactionOptimistic = (FrontendTransactionOptimistic) second.getTransaction();
    var serializer = second.getSerializer();
    for (var recordOperation : recordOperations) {
      var record = recordOperation.first;
      transactionOptimistic.addRecordOperation(serializer.fromStream(second, record, null, null),
          recordOperation.second, null);
    }

    ((AbstractPaginatedStorage) second.getStorage()).preallocateRids(transactionOptimistic);
    db.activateOnCurrentThread();
    ((AbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((FrontendTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotNull(db1.load(generated));

    db1.close();
    second.activateOnCurrentThread();
    ((AbstractPaginatedStorage) second.getStorage())
        .commitPreAllocated((FrontendTransactionOptimistic) second.getTransaction());
    second.close();
    var db2 = youTrackDB.open("secondTest", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotNull(db2.load(generated));
    db2.close();
  }

  @Test(expected = ConcurrentCreateException.class)
  public void testMultipleDbAllocationNotAlignedFailure() {
    DatabaseSessionInternal second;
    youTrackDB.execute(
        "create database "
            + "secondTest"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    second =
        (DatabaseSessionInternal)
            youTrackDB.open("secondTest", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    // THIS OFFSET FIRST DB FROM THE SECOND
    for (var i = 0; i < 20; i++) {
      second.begin();
      second.save(second.newVertex("V"));
      second.commit();
    }

    db.activateOnCurrentThread();
    db.begin();
    var v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((FrontendTransactionOptimistic) db.getTransaction());
    var transaction = db.getTransaction();
    List<RawPair<byte[], Byte>> recordOperations = new ArrayList<>();
    for (var operation : transaction.getRecordOperations()) {
      var record = operation.record;
      recordOperations.add(new RawPair<>(record.toStream(), operation.type));
    }

    second.activateOnCurrentThread();
    second.begin();
    var transactionOptimistic = (FrontendTransactionOptimistic) second.getTransaction();
    for (var recordOperation : recordOperations) {
      var record = recordOperation.first;
      var serializer = second.getSerializer();
      transactionOptimistic.addRecordOperation(serializer.fromStream(second, record, null, null),
          recordOperation.second, null);
    }
    ((AbstractPaginatedStorage) second.getStorage()).preallocateRids(transactionOptimistic);
  }

  @Test
  public void testAllocationMultipleCommit() {
    db.begin();

    List<DBRecord> orecords = new ArrayList<>();
    var v0 = db.newVertex("V");
    db.save(v0);
    for (var i = 0; i < 20; i++) {
      var v = db.newVertex("V");
      var edge = v0.addRegularEdge(v);
      orecords.add(db.save(edge));
      orecords.add(db.save(v));
    }

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((TransactionInternal) db.getTransaction());
    List<RID> allocated = new ArrayList<>();
    for (var rec : orecords) {
      allocated.add(rec.getIdentity());
    }
    ((AbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((FrontendTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    for (final var id : allocated) {
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
