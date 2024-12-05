package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @since 3/27/14
 */
public class EmbeddedEntitySerializationTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public EmbeddedEntitySerializationTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testEmbeddedObjectSerialization() {
    database.begin();
    final YTEntityImpl originalDoc = new YTEntityImpl();

    final OCompositeKey compositeKey =
        new OCompositeKey(123, "56", new Date(), new YTRecordId("#0:12"));
    originalDoc.field("compositeKey", compositeKey);
    originalDoc.field("int", 12);
    originalDoc.field("val", "test");
    originalDoc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTEntityImpl loadedDoc = database.load(originalDoc.getIdentity());
    Assert.assertNotSame(loadedDoc, originalDoc);

    final OCompositeKey loadedCompositeKey = loadedDoc.field("compositeKey");
    Assert.assertEquals(loadedCompositeKey, compositeKey);

    database.begin();
    database.bindToSession(originalDoc).delete();
    database.commit();
  }

  public void testEmbeddedObjectSerializationInsideOfOtherEmbeddedObjects() {
    final YTEntityImpl originalDoc = new YTEntityImpl();

    final OCompositeKey compositeKeyOne =
        new OCompositeKey(123, "56", new Date(), new YTRecordId("#0:12"));
    final OCompositeKey compositeKeyTwo =
        new OCompositeKey(
            245, "63", new Date(System.currentTimeMillis() + 100), new YTRecordId("#0:2"));
    final OCompositeKey compositeKeyThree =
        new OCompositeKey(
            36, "563", new Date(System.currentTimeMillis() + 1000), new YTRecordId("#0:23"));

    final YTEntityImpl embeddedDocOne = new YTEntityImpl();
    embeddedDocOne.field("compositeKey", compositeKeyOne);
    embeddedDocOne.field("val", "test");
    embeddedDocOne.field("int", 10);

    final YTEntityImpl embeddedDocTwo = new YTEntityImpl();
    embeddedDocTwo.field("compositeKey", compositeKeyTwo);
    embeddedDocTwo.field("val", "test");
    embeddedDocTwo.field("int", 10);

    final YTEntityImpl embeddedDocThree = new YTEntityImpl();
    embeddedDocThree.field("compositeKey", compositeKeyThree);
    embeddedDocThree.field("val", "test");
    embeddedDocThree.field("int", 10);

    List<YTEntityImpl> embeddedCollection = new ArrayList<YTEntityImpl>();
    embeddedCollection.add(embeddedDocTwo);
    embeddedCollection.add(embeddedDocThree);

    originalDoc.field("embeddedDoc", embeddedDocOne, YTType.EMBEDDED);
    originalDoc.field("embeddedCollection", embeddedCollection, YTType.EMBEDDEDLIST);

    database.begin();
    originalDoc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTEntityImpl loadedDocument = database.load(originalDoc.getIdentity());
    Assert.assertNotSame(loadedDocument, originalDoc);

    final YTEntityImpl loadedEmbeddedDocOne = loadedDocument.field("embeddedDoc");
    Assert.assertNotSame(loadedEmbeddedDocOne, embeddedDocOne);

    Assert.assertEquals(loadedEmbeddedDocOne.field("compositeKey"), compositeKeyOne);

    List<YTEntityImpl> loadedEmbeddedCollection = loadedDocument.field("embeddedCollection");
    Assert.assertNotSame(loadedEmbeddedCollection, embeddedCollection);

    final YTEntityImpl loadedEmbeddedDocTwo = loadedEmbeddedCollection.get(0);
    Assert.assertNotSame(loadedEmbeddedDocTwo, embeddedDocTwo);

    Assert.assertEquals(loadedEmbeddedDocTwo.field("compositeKey"), compositeKeyTwo);

    final YTEntityImpl loadedEmbeddedDocThree = loadedEmbeddedCollection.get(1);
    Assert.assertNotSame(loadedEmbeddedDocThree, embeddedDocThree);

    Assert.assertEquals(loadedEmbeddedDocThree.field("compositeKey"), compositeKeyThree);

    database.begin();
    database.bindToSession(originalDoc).delete();
    database.commit();
  }
}
