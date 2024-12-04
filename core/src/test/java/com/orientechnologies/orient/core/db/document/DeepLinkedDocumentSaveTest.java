package com.orientechnologies.orient.core.db.document;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class DeepLinkedDocumentSaveTest extends DBTestBase {

  @Test
  public void testLinkedTx() {
    final Set<YTDocument> docs = new HashSet<>();

    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    YTDocument doc = new YTDocument("Test");
    docs.add(doc);
    for (int i = 0; i < 3000; i++) {
      docs.add(doc = new YTDocument("Test").field("linked", doc));
    }
    db.save(doc);
    db.commit();

    assertEquals(3001, db.countClass("Test"));

    for (YTDocument d : docs) {
      assertEquals(1, d.getVersion());
    }
  }
}
