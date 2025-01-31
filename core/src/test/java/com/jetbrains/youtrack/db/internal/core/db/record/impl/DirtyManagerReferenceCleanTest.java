package com.jetbrains.youtrack.db.internal.core.db.record.impl;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

public class DirtyManagerReferenceCleanTest extends DbTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();

    db.getMetadata().getSchema().createClass("test");
  }

  @Test
  public void testReferDeletedDocument() {
    var id = db.computeInTx(() -> {
      var doc = (EntityImpl) db.newEntity();
      var doc1 = (EntityImpl) db.newEntity();
      doc1.field("aa", "aa");
      doc.field("ref", doc1);
      doc.field("bb");

      doc.save();
      return doc.getIdentity();
    });

    var rid1 = db.computeInTx(() -> {
      var doc = db.loadEntity(id.getIdentity());
      var doc1 = doc.getEntityProperty("ref");
      doc1.delete();
      doc.setProperty("ab", "ab");
      return doc1.getIdentity();
    });

    db.executeInTx(() -> {
      try {
        db.loadEntity(rid1);
        Assert.fail();
      } catch (RecordNotFoundException e) {
        //
      }
      var doc = db.loadEntity(id.getIdentity());
      Assert.assertEquals("ab", doc.getProperty("ab"));
    });
  }
}
