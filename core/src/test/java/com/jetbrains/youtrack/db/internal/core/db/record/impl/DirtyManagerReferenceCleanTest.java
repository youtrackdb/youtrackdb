package com.jetbrains.youtrack.db.internal.core.db.record.impl;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DirtyManagerReferenceCleanTest extends DbTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();

    db.getMetadata().getSchema().createClass("test");
  }

  @Test
  public void testReferDeletedDocument() {
    db.begin();
    EntityImpl doc = new EntityImpl();
    EntityImpl doc1 = new EntityImpl();
    doc1.field("aa", "aa");
    doc.field("ref", doc1);
    doc.field("bb");

    doc.save(db.getClusterNameById(db.getDefaultClusterId()));
    Identifiable id = doc.getIdentity();
    db.commit();

    db.begin();
    doc = db.load(id.getIdentity());
    doc1 = doc.field("ref");
    doc1.delete();
    doc.field("ab", "ab");
    Assert.assertFalse(RecordInternal.getDirtyManager(doc).getUpdateRecords().contains(doc1));
    db.commit();
  }
}
