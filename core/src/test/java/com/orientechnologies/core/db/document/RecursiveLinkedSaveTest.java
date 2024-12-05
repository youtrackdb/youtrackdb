package com.orientechnologies.core.db.document;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import org.junit.Test;

/**
 *
 */
public class RecursiveLinkedSaveTest extends DBTestBase {

  @Test
  public void testTxLinked() {
    db.getMetadata().getSchema().createClass("Test");
    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Test");
    YTEntityImpl doc1 = new YTEntityImpl("Test");
    doc.field("link", doc1);
    YTEntityImpl doc2 = new YTEntityImpl("Test");
    doc1.field("link", doc2);
    doc2.field("link", doc);
    db.save(doc);
    db.commit();
    assertEquals(3, db.countClass("Test"));
    doc = db.load(doc.getIdentity());
    doc1 = doc.field("link");
    doc2 = doc1.field("link");
    assertEquals(doc, doc2.field("link"));
  }
}
