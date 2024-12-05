package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 *
 */
public class IndexTxTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public IndexTxTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass cls = schema.createClass("IndexTxTestClass");
    cls.createProperty(database, "name", YTType.STRING);
    cls.createIndex(database, "IndexTxTestIndex", YTClass.INDEX_TYPE.UNIQUE, "name");
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass cls = schema.getClass("IndexTxTestClass");
    if (cls != null) {
      cls.truncate(database);
    }
  }

  @Test
  public void testIndexCrossReferencedDocuments() {
    checkEmbeddedDB();

    database.begin();

    final YTEntityImpl doc1 = new YTEntityImpl("IndexTxTestClass");
    final YTEntityImpl doc2 = new YTEntityImpl("IndexTxTestClass");

    doc1.save();
    doc2.save();

    doc1.field("ref", doc2.getIdentity().copy());
    doc1.field("name", "doc1");
    doc2.field("ref", doc1.getIdentity().copy());
    doc2.field("name", "doc2");

    doc1.save();
    doc2.save();

    database.commit();

    Map<String, YTRID> expectedResult = new HashMap<>();
    expectedResult.put("doc1", doc1.getIdentity());
    expectedResult.put("doc2", doc2.getIdentity());

    OIndex index = getIndex("IndexTxTestIndex");
    Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        String key = (String) keyIterator.next();

        final YTRID expectedValue = expectedResult.get(key);
        final YTRID value;
        try (Stream<YTRID> stream = index.getInternal().getRids(database, key)) {
          value = stream.findAny().orElse(null);
        }

        Assert.assertNotNull(value);
        Assert.assertTrue(value.isPersistent());
        Assert.assertEquals(value, expectedValue);
      }
    }
  }
}
