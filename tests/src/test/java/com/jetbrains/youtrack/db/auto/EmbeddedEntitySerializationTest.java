package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @since 3/27/14
 */
public class EmbeddedEntitySerializationTest extends BaseDBTest {

  @Parameters(value = "remote")
  public EmbeddedEntitySerializationTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testEmbeddedObjectSerialization() {
    session.begin();
    final var originalDoc = ((EntityImpl) session.newEntity());

    final var compositeKey =
        new CompositeKey(123, "56", new Date(), new RecordId("#0:12"));
    originalDoc.field("compositeKey", compositeKey);
    originalDoc.field("int", 12);
    originalDoc.field("val", "test");

    session.commit();

    final EntityImpl loadedDoc = session.load(originalDoc.getIdentity());
    Assert.assertNotSame(loadedDoc, originalDoc);

    final CompositeKey loadedCompositeKey = loadedDoc.field("compositeKey");
    Assert.assertEquals(loadedCompositeKey, compositeKey);

    session.begin();
    session.bindToSession(originalDoc).delete();
    session.commit();
  }

  public void testEmbeddedObjectSerializationInsideOfOtherEmbeddedObjects() {
    final var originalDoc = ((EntityImpl) session.newEntity());

    final var compositeKeyOne =
        new CompositeKey(123, "56", new Date(), new RecordId("#0:12"));
    final var compositeKeyTwo =
        new CompositeKey(
            245, "63", new Date(System.currentTimeMillis() + 100), new RecordId("#0:2"));
    final var compositeKeyThree =
        new CompositeKey(
            36, "563", new Date(System.currentTimeMillis() + 1000), new RecordId("#0:23"));

    final var embeddedDocOne = ((EntityImpl) session.newEntity());
    embeddedDocOne.field("compositeKey", compositeKeyOne);
    embeddedDocOne.field("val", "test");
    embeddedDocOne.field("int", 10);

    final var embeddedDocTwo = ((EntityImpl) session.newEntity());
    embeddedDocTwo.field("compositeKey", compositeKeyTwo);
    embeddedDocTwo.field("val", "test");
    embeddedDocTwo.field("int", 10);

    final var embeddedDocThree = ((EntityImpl) session.newEntity());
    embeddedDocThree.field("compositeKey", compositeKeyThree);
    embeddedDocThree.field("val", "test");
    embeddedDocThree.field("int", 10);

    List<EntityImpl> embeddedCollection = new ArrayList<EntityImpl>();
    embeddedCollection.add(embeddedDocTwo);
    embeddedCollection.add(embeddedDocThree);

    originalDoc.field("embeddedDoc", embeddedDocOne, PropertyType.EMBEDDED);
    originalDoc.field("embeddedCollection", embeddedCollection, PropertyType.EMBEDDEDLIST);

    session.begin();

    session.commit();

    final EntityImpl loadedDocument = session.load(originalDoc.getIdentity());
    Assert.assertNotSame(loadedDocument, originalDoc);

    final EntityImpl loadedEmbeddedDocOne = loadedDocument.field("embeddedDoc");
    Assert.assertNotSame(loadedEmbeddedDocOne, embeddedDocOne);

    Assert.assertEquals(loadedEmbeddedDocOne.field("compositeKey"), compositeKeyOne);

    List<EntityImpl> loadedEmbeddedCollection = loadedDocument.field("embeddedCollection");
    Assert.assertNotSame(loadedEmbeddedCollection, embeddedCollection);

    final var loadedEmbeddedDocTwo = loadedEmbeddedCollection.get(0);
    Assert.assertNotSame(loadedEmbeddedDocTwo, embeddedDocTwo);

    Assert.assertEquals(loadedEmbeddedDocTwo.field("compositeKey"), compositeKeyTwo);

    final var loadedEmbeddedDocThree = loadedEmbeddedCollection.get(1);
    Assert.assertNotSame(loadedEmbeddedDocThree, embeddedDocThree);

    Assert.assertEquals(loadedEmbeddedDocThree.field("compositeKey"), compositeKeyThree);

    session.begin();
    session.bindToSession(originalDoc).delete();
    session.commit();
  }
}
