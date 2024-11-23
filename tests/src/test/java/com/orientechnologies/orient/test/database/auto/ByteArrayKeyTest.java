package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

    final OClass byteArrayKeyTest =
        database.getMetadata().getSchema().createClass("ByteArrayKeyTest");
    byteArrayKeyTest.createProperty(database, "byteArrayKey", OType.BINARY);

    byteArrayKeyTest.createIndex(database, "byteArrayKeyIndex", OClass.INDEX_TYPE.UNIQUE,
        "byteArrayKey");

    final OClass compositeByteArrayKeyTest =
        database.getMetadata().getSchema().createClass("CompositeByteArrayKeyTest");
    compositeByteArrayKeyTest.createProperty(database, "byteArrayKey", OType.BINARY);
    compositeByteArrayKeyTest.createProperty(database, "intKey", OType.INTEGER);

    compositeByteArrayKeyTest.createIndex(database,
        "compositeByteArrayKey", OClass.INDEX_TYPE.UNIQUE, "byteArrayKey", "intKey");
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
    ODocument doc1 = new ODocument("ByteArrayKeyTest");
    doc1.field("byteArrayKey", key1);
    doc1.save();

    byte[] key2 =
        new byte[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8,
            9,
            0, 2
        };
    ODocument doc2 = new ODocument("ByteArrayKeyTest");
    doc2.field("byteArrayKey", key2);
    doc2.save();
    database.commit();

    OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, "byteArrayKeyIndex");
    try (Stream<ORID> stream = index.getInternal().getRids(database, key1)) {
      Assert.assertEquals(stream.findAny().map(ORID::getRecord).orElse(null), doc1);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(database, key2)) {
      Assert.assertEquals(stream.findAny().map(ORID::getRecord).orElse(null), doc2);
    }
  }

  public void testAutomaticCompositeUsage() {
    checkEmbeddedDB();

    byte[] key1 = new byte[]{1, 2, 3};
    byte[] key2 = new byte[]{4, 5, 6};

    database.begin();
    ODocument doc1 = new ODocument("CompositeByteArrayKeyTest");
    doc1.field("byteArrayKey", key1);
    doc1.field("intKey", 1);
    doc1.save();

    ODocument doc2 = new ODocument("CompositeByteArrayKeyTest");
    doc2.field("byteArrayKey", key2);
    doc2.field("intKey", 2);
    doc2.save();
    database.commit();

    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "compositeByteArrayKey");
    try (Stream<ORID> stream = index.getInternal().getRids(database, new OCompositeKey(key1, 1))) {
      Assert.assertEquals(stream.findAny().map(ORID::getRecord).orElse(null), doc1);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(database, new OCompositeKey(key2, 2))) {
      Assert.assertEquals(stream.findAny().map(ORID::getRecord).orElse(null), doc2);
    }
  }

  public void testAutomaticCompositeUsageInTX() {
    checkEmbeddedDB();

    byte[] key1 = new byte[]{7, 8, 9};
    byte[] key2 = new byte[]{10, 11, 12};

    database.begin();
    ODocument doc1 = new ODocument("CompositeByteArrayKeyTest");
    doc1.field("byteArrayKey", key1);
    doc1.field("intKey", 1);
    doc1.save();

    ODocument doc2 = new ODocument("CompositeByteArrayKeyTest");
    doc2.field("byteArrayKey", key2);
    doc2.field("intKey", 2);
    doc2.save();
    database.commit();

    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "compositeByteArrayKey");
    try (Stream<ORID> stream = index.getInternal().getRids(database, new OCompositeKey(key1, 1))) {
      Assert.assertEquals(stream.findAny().map(ORID::getRecord).orElse(null), doc1);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(database, new OCompositeKey(key2, 2))) {
      Assert.assertEquals(stream.findAny().map(ORID::getRecord).orElse(null), doc2);
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
    try (Stream<ORID> stream = autoIndex.getInternal().getRids(database, key1)) {
      Assert.assertTrue(stream.findFirst().isPresent());
    }
    try (Stream<ORID> stream = autoIndex.getInternal().getRids(database, key2)) {
      Assert.assertTrue(stream.findFirst().isPresent());
    }
  }
}
