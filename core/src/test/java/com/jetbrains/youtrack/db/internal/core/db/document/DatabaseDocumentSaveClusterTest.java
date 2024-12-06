package com.jetbrains.youtrack.db.internal.core.db.document;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DatabaseDocumentSaveClusterTest extends DbTestBase {

  @Test(expected = IllegalArgumentException.class)
  public void testSaveWrongCluster() {
    db.getMetadata().getSchema().createClass("test");
    db.addCluster("test_one");

    db.save(new EntityImpl("test"), "test_one");
  }

  @Test(expected = SchemaException.class)
  public void testUsedClusterTest() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("test");
    db.addCluster("test_one");
    clazz.addCluster(db, "test_one");
    SchemaClass clazz2 = db.getMetadata().getSchema().createClass("test2");
    clazz2.addCluster(db, "test_one");
  }

  @Test
  public void testSaveCluster() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("test");
    int res = db.addCluster("test_one");
    clazz.addCluster(db, "test_one");

    Record saved = db.computeInTx(() -> db.save(new EntityImpl("test"), "test_one"));
    Assert.assertEquals(saved.getIdentity().getClusterId(), res);
  }

  @Test
  public void testDeleteClassAndClusters() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("test");
    int res = db.addCluster("test_one");
    clazz.addCluster(db, "test_one");

    Record saved = db.computeInTx(() -> db.save(new EntityImpl("test"), "test_one"));
    Assert.assertEquals(saved.getIdentity().getClusterId(), res);
    db.getMetadata().getSchema().dropClass(clazz.getName());
    Assert.assertFalse(db.existsCluster("test_one"));
  }
}
