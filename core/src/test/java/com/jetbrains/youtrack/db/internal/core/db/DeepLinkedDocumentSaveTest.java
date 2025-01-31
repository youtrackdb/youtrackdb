package com.jetbrains.youtrack.db.internal.core.db;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class DeepLinkedDocumentSaveTest extends DbTestBase {

  @Test
  public void testLinkedTx() {
    final Set<EntityImpl> docs = new HashSet<>();

    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    var doc = (EntityImpl) db.newEntity("Test");
    docs.add(doc);
    for (var i = 0; i < 3000; i++) {
      docs.add(doc = ((EntityImpl) db.newEntity("Test")).field("linked", doc));
    }
    db.save(doc);
    db.commit();

    assertEquals(3001, db.countClass("Test"));

    for (var d : docs) {
      assertEquals(1, d.getVersion());
    }
  }
}
