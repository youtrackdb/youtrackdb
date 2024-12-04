package com.orientechnologies.orient.core.tx;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
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
  private ODatabaseSessionInternal db;

  @Before
  public void before() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    db =
        (ODatabaseSessionInternal)
            youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void testAllocation() {
    db.begin();
    OVertex v = db.newVertex("V");
    db.save(v);

    ((OAbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionInternal) db.getTransaction());
    ORID generated = v.getIdentity();
    assertTrue(generated.isValid());

    var db1 = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    try {
      db1.load(generated);
      Assert.fail();
    } catch (ORecordNotFoundException e) {
      // ignore
    }

    db1.close();
  }

  @Test
  public void testAllocationCommit() {
    db.begin();
    OVertex v = db.newVertex("V");
    db.save(v);

    ((OAbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionOptimistic) db.getTransaction());
    ORID generated = v.getIdentity();
    ((OAbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((OTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertNotNull(db1.load(generated));
    db1.close();
  }

  @Test
  public void testMultipleDbAllocationAndCommit() {
    ODatabaseSessionInternal second;
    youTrackDB.execute(
        "create database "
            + "secondTest"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    second =
        (ODatabaseSessionInternal)
            youTrackDB.open("secondTest", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    db.activateOnCurrentThread();
    db.begin();
    OVertex v = db.newVertex("V");
    db.save(v);

    ((OAbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionInternal) db.getTransaction());
    ORID generated = v.getIdentity();
    OTransaction transaction = db.getTransaction();
    List<ORawPair<ORecordAbstract, Byte>> recordOperations = new ArrayList<>();
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

    ((OAbstractPaginatedStorage) second.getStorage()).preallocateRids(transactionOptimistic);
    db.activateOnCurrentThread();
    ((OAbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((OTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotNull(db1.load(generated));

    db1.close();
    second.activateOnCurrentThread();
    ((OAbstractPaginatedStorage) second.getStorage())
        .commitPreAllocated((OTransactionOptimistic) second.getTransaction());
    second.close();
    var db2 = youTrackDB.open("secondTest", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotNull(db2.load(generated));
    db2.close();
  }

  @Test(expected = OConcurrentCreateException.class)
  public void testMultipleDbAllocationNotAlignedFailure() {
    ODatabaseSessionInternal second;
    youTrackDB.execute(
        "create database "
            + "secondTest"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    second =
        (ODatabaseSessionInternal)
            youTrackDB.open("secondTest", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    // THIS OFFSET FIRST DB FROM THE SECOND
    for (int i = 0; i < 20; i++) {
      second.begin();
      second.save(second.newVertex("V"));
      second.commit();
    }

    db.activateOnCurrentThread();
    db.begin();
    OVertex v = db.newVertex("V");
    db.save(v);

    ((OAbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionOptimistic) db.getTransaction());
    OTransaction transaction = db.getTransaction();
    List<ORawPair<ORecordAbstract, Byte>> recordOperations = new ArrayList<>();
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
    ((OAbstractPaginatedStorage) second.getStorage()).preallocateRids(transactionOptimistic);
  }

  @Test
  public void testAllocationMultipleCommit() {
    db.begin();

    List<ORecord> orecords = new ArrayList<>();
    OVertex v0 = db.newVertex("V");
    db.save(v0);
    for (int i = 0; i < 20; i++) {
      OVertex v = db.newVertex("V");
      OEdge edge = v0.addEdge(v);
      orecords.add(db.save(edge));
      orecords.add(db.save(v));
    }

    ((OAbstractPaginatedStorage) db.getStorage())
        .preallocateRids((OTransactionInternal) db.getTransaction());
    List<ORID> allocated = new ArrayList<>();
    for (ORecord rec : orecords) {
      allocated.add(rec.getIdentity());
    }
    ((OAbstractPaginatedStorage) db.getStorage())
        .commitPreAllocated((OTransactionOptimistic) db.getTransaction());

    var db1 = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    for (final ORID id : allocated) {
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
