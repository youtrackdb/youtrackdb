package com.jetbrains.youtrack.db.internal.core.record.impl;


import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Tests that {@link EntityImpl} is serializable.
 *
 * @since 12/20/12
 */
public class DocumentSerializationPersistentTest extends BaseMemoryInternalDatabase {

  public void beforeTest() throws Exception {
    super.beforeTest();

    db.begin();
    final var doc = (EntityImpl) db.newEntity();
    doc.setProperty("name", "Artem");

    final var linkedDoc = (EntityImpl) db.newEntity();

    doc.setProperty("country", linkedDoc, PropertyType.LINK);
    doc.setProperty("numbers", Arrays.asList(0, 1, 2, 3, 4, 5));
    doc.save();

    db.commit();
  }

  @Test(expected = DatabaseException.class)
  public void testRidBagInEmbeddedDocument() {
    DatabaseRecordThreadLocal.instance().set(db);
    var doc = (EntityImpl) db.newEntity();
    var rids = new RidBag(db);
    rids.add(new RecordId(2, 3));
    rids.add(new RecordId(2, 4));
    rids.add(new RecordId(2, 5));
    rids.add(new RecordId(2, 6));
    List<EntityImpl> docs = new ArrayList<EntityImpl>();
    var doc1 = (EntityImpl) db.newEntity();
    doc1.setProperty("rids", rids);
    docs.add(doc1);
    var doc2 = (EntityImpl) db.newEntity();
    doc2.setProperty("text", "text");
    docs.add(doc2);
    doc.setProperty("emb", docs, PropertyType.EMBEDDEDLIST);
    doc.setProperty("some", "test");

    var res = db.getSerializer().toStream(db, doc);
    db.getSerializer().fromStream(db, res, (EntityImpl) db.newEntity(), new String[]{});
  }
}
