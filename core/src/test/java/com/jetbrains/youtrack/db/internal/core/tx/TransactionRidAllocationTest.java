package com.jetbrains.youtrack.db.internal.core.tx;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.exception.ConcurrentCreateException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
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
    Vertex v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((TransactionInternal) db.getTransaction());
    RecordId generated = (RecordId) v.getIdentity();
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
    Vertex v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((FrontendTransactionOptimistic) db.getTransaction());
    RID generated = v.getIdentity();
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
    Vertex v = db.newVertex("V");
    db.save(v);

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((TransactionInternal) db.getTransaction());
    RID generated = v.getIdentity();
    FrontendTransaction transaction = db.getTransaction();
    List<RawPair<RecordAbstract, Byte>> recordOperations = new ArrayList<>();
    for (RecordOperation operation : transaction.getRecordOperations()) {
      var record = operation.record;
      recordOperations.add(new RawPair<>(record.copy(), operation.type));
    }

    second.activateOnCurrentThread();
    second.begin();
    FrontendTransactionOptimistic transactionOptimistic = (FrontendTransactionOptimistic) second.getTransaction();
    for (var recordOperation : recordOperations) {
      var record = recordOperation.first;
      record.setup(second);
      transactionOptimistic.addRecordOperation(record, recordOperation.second, null);
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
        .preallocateRids((FrontendTransactionOptimistic) db.getTransaction());
    FrontendTransaction transaction = db.getTransaction();
    List<RawPair<RecordAbstract, Byte>> recordOperations = new ArrayList<>();
    for (RecordOperation operation : transaction.getRecordOperations()) {
      var record = operation.record;
      recordOperations.add(new RawPair<>(record.copy(), operation.type));
    }

    second.activateOnCurrentThread();
    second.begin();
    FrontendTransactionOptimistic transactionOptimistic = (FrontendTransactionOptimistic) second.getTransaction();
    for (var recordOperation : recordOperations) {
      var record = recordOperation.first;
      record.setup(second);
      transactionOptimistic.addRecordOperation(record, recordOperation.second, null);
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
      Edge edge = v0.addRegularEdge(v);
      orecords.add(db.save(edge));
      orecords.add(db.save(v));
    }

    ((AbstractPaginatedStorage) db.getStorage())
        .preallocateRids((TransactionInternal) db.getTransaction());
    List<RID> allocated = new ArrayList<>();
    for (Record rec : orecords) {
      allocated.add(rec.getIdentity());
    }
    ((AbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((FrontendTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    for (final RID id : allocated) {
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
