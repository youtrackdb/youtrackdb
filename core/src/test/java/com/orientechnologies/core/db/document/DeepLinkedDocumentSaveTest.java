package com.orientechnologies.core.db.document;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class DeepLinkedDocumentSaveTest extends DBTestBase {

  @Test
  public void testLinkedTx() {
    final Set<YTEntityImpl> docs = new HashSet<>();

    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Test");
    docs.add(doc);
    for (int i = 0; i < 3000; i++) {
      docs.add(doc = new YTEntityImpl("Test").field("linked", doc));
    }
    db.save(doc);
    db.commit();

    assertEquals(3001, db.countClass("Test"));

    for (YTEntityImpl d : docs) {
      assertEquals(1, d.getVersion());
    }
  }
}
