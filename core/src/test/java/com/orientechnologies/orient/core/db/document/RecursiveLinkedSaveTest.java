package com.orientechnologies.orient.core.db.document;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import org.junit.Test;

/**
 *
 */
public class RecursiveLinkedSaveTest extends DBTestBase {

  @Test
  public void testTxLinked() {
    db.getMetadata().getSchema().createClass("Test");
    db.begin();
    YTDocument doc = new YTDocument("Test");
    YTDocument doc1 = new YTDocument("Test");
    doc.field("link", doc1);
    YTDocument doc2 = new YTDocument("Test");
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
