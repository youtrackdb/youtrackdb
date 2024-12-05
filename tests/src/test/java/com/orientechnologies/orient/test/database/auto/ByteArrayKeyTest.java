package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OCompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 03.07.12
 */
@Test
public class ByteArrayKeyTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public ByteArrayKeyTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final YTClass byteArrayKeyTest =
        database.getMetadata().getSchema().createClass("ByteArrayKeyTest");
    byteArrayKeyTest.createProperty(database, "byteArrayKey", YTType.BINARY);

    byteArrayKeyTest.createIndex(database, "byteArrayKeyIndex", YTClass.INDEX_TYPE.UNIQUE,
        "byteArrayKey");

    final YTClass compositeByteArrayKeyTest =
        database.getMetadata().getSchema().createClass("CompositeByteArrayKeyTest");
    compositeByteArrayKeyTest.createProperty(database, "byteArrayKey", YTType.BINARY);
    compositeByteArrayKeyTest.createProperty(database, "intKey", YTType.INTEGER);

    compositeByteArrayKeyTest.createIndex(database,
        "compositeByteArrayKey", YTClass.INDEX_TYPE.UNIQUE, "byteArrayKey", "intKey");
  }

  public void testAutomaticUsage() {
    checkEmbeddedDB();

    byte[] key1 =
        new byte[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8,
            9,
            0, 1
        };

    database.begin();
    EntityImpl doc1 = new EntityImpl("ByteArrayKeyTest");
    doc1.field("byteArrayKey", key1);
    doc1.save();

    byte[] key2 =
        new byte[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8,
            9,
            0, 2
        };
    EntityImpl doc2 = new EntityImpl("ByteArrayKeyTest");
    doc2.field("byteArrayKey", key2);
    doc2.save();
    database.commit();

    OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, "byteArrayKeyIndex");
    try (Stream<YTRID> stream = index.getInternal().getRids(database, key1)) {
      Assert.assertEquals(stream.findAny().map(YTRID::getRecord).orElse(null), doc1);
    }
    try (Stream<YTRID> stream = index.getInternal().getRids(database, key2)) {
      Assert.assertEquals(stream.findAny().map(YTRID::getRecord).orElse(null), doc2);
    }
  }

  public void testAutomaticCompositeUsage() {
    checkEmbeddedDB();

    byte[] key1 = new byte[]{1, 2, 3};
    byte[] key2 = new byte[]{4, 5, 6};

    database.begin();
    EntityImpl doc1 = new EntityImpl("CompositeByteArrayKeyTest");
    doc1.field("byteArrayKey", key1);
    doc1.field("intKey", 1);
    doc1.save();

    EntityImpl doc2 = new EntityImpl("CompositeByteArrayKeyTest");
    doc2.field("byteArrayKey", key2);
    doc2.field("intKey", 2);
    doc2.save();
    database.commit();

    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "compositeByteArrayKey");
    try (Stream<YTRID> stream = index.getInternal().getRids(database, new OCompositeKey(key1, 1))) {
      Assert.assertEquals(stream.findAny().map(YTRID::getRecord).orElse(null), doc1);
    }
    try (Stream<YTRID> stream = index.getInternal().getRids(database, new OCompositeKey(key2, 2))) {
      Assert.assertEquals(stream.findAny().map(YTRID::getRecord).orElse(null), doc2);
    }
  }

  public void testAutomaticCompositeUsageInTX() {
    checkEmbeddedDB();

    byte[] key1 = new byte[]{7, 8, 9};
    byte[] key2 = new byte[]{10, 11, 12};

    database.begin();
    EntityImpl doc1 = new EntityImpl("CompositeByteArrayKeyTest");
    doc1.field("byteArrayKey", key1);
    doc1.field("intKey", 1);
    doc1.save();

    EntityImpl doc2 = new EntityImpl("CompositeByteArrayKeyTest");
    doc2.field("byteArrayKey", key2);
    doc2.field("intKey", 2);
    doc2.save();
    database.commit();

    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "compositeByteArrayKey");
    try (Stream<YTRID> stream = index.getInternal().getRids(database, new OCompositeKey(key1, 1))) {
      Assert.assertEquals(stream.findAny().map(YTRID::getRecord).orElse(null), doc1);
    }
    try (Stream<YTRID> stream = index.getInternal().getRids(database, new OCompositeKey(key2, 2))) {
      Assert.assertEquals(stream.findAny().map(YTRID::getRecord).orElse(null), doc2);
    }
  }

  @Test(dependsOnMethods = {"testAutomaticUsage"})
  public void testContains() {
    byte[] key1 =
        new byte[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8,
            9,
            0, 1
        };
    byte[] key2 =
        new byte[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8,
            9,
            0, 2
        };

    OIndex autoIndex =
        database.getMetadata().getIndexManagerInternal().getIndex(database, "byteArrayKeyIndex");
    try (Stream<YTRID> stream = autoIndex.getInternal().getRids(database, key1)) {
      Assert.assertTrue(stream.findFirst().isPresent());
    }
    try (Stream<YTRID> stream = autoIndex.getInternal().getRids(database, key2)) {
      Assert.assertTrue(stream.findFirst().isPresent());
    }
  }
}
