package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.exception.YTSchemaException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODatabaseDocumentSaveClusterTest extends DBTestBase {

  @Test(expected = IllegalArgumentException.class)
  public void testSaveWrongCluster() {
    db.getMetadata().getSchema().createClass("test");
    db.addCluster("test_one");

    db.save(new YTEntityImpl("test"), "test_one");
  }

  @Test(expected = YTSchemaException.class)
  public void testUsedClusterTest() {
    YTClass clazz = db.getMetadata().getSchema().createClass("test");
    db.addCluster("test_one");
    clazz.addCluster(db, "test_one");
    YTClass clazz2 = db.getMetadata().getSchema().createClass("test2");
    clazz2.addCluster(db, "test_one");
  }

  @Test
  public void testSaveCluster() {
    YTClass clazz = db.getMetadata().getSchema().createClass("test");
    int res = db.addCluster("test_one");
    clazz.addCluster(db, "test_one");

    YTRecord saved = db.computeInTx(() -> db.save(new YTEntityImpl("test"), "test_one"));
    Assert.assertEquals(saved.getIdentity().getClusterId(), res);
  }

  @Test
  public void testDeleteClassAndClusters() {
    YTClass clazz = db.getMetadata().getSchema().createClass("test");
    int res = db.addCluster("test_one");
    clazz.addCluster(db, "test_one");

    YTRecord saved = db.computeInTx(() -> db.save(new YTEntityImpl("test"), "test_one"));
    Assert.assertEquals(saved.getIdentity().getClusterId(), res);
    db.getMetadata().getSchema().dropClass(clazz.getName());
    Assert.assertFalse(db.existsCluster("test_one"));
  }
}
