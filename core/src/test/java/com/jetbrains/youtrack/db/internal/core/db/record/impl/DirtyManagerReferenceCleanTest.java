package com.jetbrains.youtrack.db.internal.core.db.record.impl;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

public class DirtyManagerReferenceCleanTest extends DbTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();

    session.getMetadata().getSchema().createClass("test");
  }

  @Test
  public void testReferDeletedDocument() {
    var id = session.computeInTx(() -> {
      var doc = (EntityImpl) session.newEntity();
      var doc1 = (EntityImpl) session.newEntity();
      doc1.field("aa", "aa");
      doc.field("ref", doc1);
      doc.field("bb");

      doc.save();
      return doc.getIdentity();
    });

    var rid1 = session.computeInTx(() -> {
      var doc = session.loadEntity(id.getIdentity());
      var doc1 = doc.getEntity("ref");
      doc1.delete();
      doc.setProperty("ab", "ab");
      return doc1.getIdentity();
    });

    session.executeInTx(() -> {
      try {
        session.loadEntity(rid1);
        Assert.fail();
      } catch (RecordNotFoundException e) {
        //
      }
      var doc = session.loadEntity(id.getIdentity());
      Assert.assertEquals("ab", doc.getProperty("ab"));
    });
  }
}
