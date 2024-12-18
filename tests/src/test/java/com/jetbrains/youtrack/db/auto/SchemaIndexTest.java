package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SchemaIndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SchemaIndexTest(boolean remote) {
    super(remote);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass superTest = schema.createClass("SchemaSharedIndexSuperTest");
    final SchemaClass test = schema.createClass("SchemaIndexTest", superTest);
    test.createProperty(db, "prop1", PropertyType.DOUBLE);
    test.createProperty(db, "prop2", PropertyType.DOUBLE);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (db.getMetadata().getSchema().existsClass("SchemaIndexTest")) {
      db.command("drop class SchemaIndexTest").close();
    }
    db.command("drop class SchemaSharedIndexSuperTest").close();
  }

  @Test
  public void testDropClass() throws Exception {
    db
        .command(
            "CREATE INDEX SchemaSharedIndexCompositeIndex ON SchemaIndexTest (prop1, prop2) UNIQUE")
        .close();
    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNotNull(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "SchemaSharedIndexCompositeIndex"));

    db.getMetadata().getSchema().dropClass("SchemaIndexTest");
    db.getMetadata().getIndexManagerInternal().reload(db);

    Assert.assertNull(db.getMetadata().getSchema().getClass("SchemaIndexTest"));
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("SchemaSharedIndexSuperTest"));

    Assert.assertNull(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "SchemaSharedIndexCompositeIndex"));
  }

  @Test
  public void testDropSuperClass() throws Exception {
    db
        .command(
            "CREATE INDEX SchemaSharedIndexCompositeIndex ON SchemaIndexTest (prop1, prop2) UNIQUE")
        .close();

    try {
      db.getMetadata().getSchema().dropClass("SchemaSharedIndexSuperTest");
      Assert.fail();
    } catch (SchemaException e) {
      Assert.assertTrue(
          e.getMessage()
              .startsWith(
                  "Class 'SchemaSharedIndexSuperTest' cannot be dropped because it has sub"
                      + " classes"));
    }

    Assert.assertNotNull(db.getMetadata().getSchema().getClass("SchemaIndexTest"));
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("SchemaSharedIndexSuperTest"));

    Assert.assertNotNull(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "SchemaSharedIndexCompositeIndex"));
  }

  public void testPolymorphicIdsPropagationAfterClusterAddRemove() {
    final Schema schema = db.getMetadata().getSchema();

    SchemaClass polymorpicIdsPropagationSuperSuper =
        schema.getClass("polymorpicIdsPropagationSuperSuper");

    if (polymorpicIdsPropagationSuperSuper == null) {
      polymorpicIdsPropagationSuperSuper = schema.createClass("polymorpicIdsPropagationSuperSuper");
    }

    SchemaClass polymorpicIdsPropagationSuper = schema.getClass("polymorpicIdsPropagationSuper");
    if (polymorpicIdsPropagationSuper == null) {
      polymorpicIdsPropagationSuper = schema.createClass("polymorpicIdsPropagationSuper");
    }

    SchemaClass polymorpicIdsPropagation = schema.getClass("polymorpicIdsPropagation");
    if (polymorpicIdsPropagation == null) {
      polymorpicIdsPropagation = schema.createClass("polymorpicIdsPropagation");
    }

    polymorpicIdsPropagation.setSuperClass(db, polymorpicIdsPropagationSuper);
    polymorpicIdsPropagationSuper.setSuperClass(db, polymorpicIdsPropagationSuperSuper);

    polymorpicIdsPropagationSuperSuper.createProperty(db, "value", PropertyType.STRING);
    polymorpicIdsPropagationSuperSuper.createIndex(db,
        "PolymorpicIdsPropagationSuperSuperIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value");

    int counter = 0;

    for (int i = 0; i < 10; i++) {
      db.begin();
      EntityImpl document = ((EntityImpl) db.newEntity("polymorpicIdsPropagation"));
      document.field("value", "val" + counter);
      document.save();
      db.commit();

      counter++;
    }

    final int clusterId2 = db.addCluster("polymorpicIdsPropagationSuperSuper2");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity());
      document.field("value", "val" + counter);

      db.begin();
      document.save("polymorpicIdsPropagationSuperSuper2");
      db.commit();

      counter++;
    }

    polymorpicIdsPropagation.addCluster(db, "polymorpicIdsPropagationSuperSuper2");

    assertContains(polymorpicIdsPropagationSuperSuper.getPolymorphicClusterIds(), clusterId2);
    assertContains(polymorpicIdsPropagationSuper.getPolymorphicClusterIds(), clusterId2);

    try (ResultSet result =
        db.query("select from polymorpicIdsPropagationSuperSuper where value = 'val12'")) {

      Assert.assertTrue(result.hasNext());

      Result doc = result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertEquals(doc.getProperty("@class"), "polymorpicIdsPropagation");
    }
    polymorpicIdsPropagation.removeClusterId(db, clusterId2);

    assertDoesNotContain(polymorpicIdsPropagationSuperSuper.getPolymorphicClusterIds(), clusterId2);
    assertDoesNotContain(polymorpicIdsPropagationSuper.getPolymorphicClusterIds(), clusterId2);

    try (ResultSet result =
        db.query("select from polymorpicIdsPropagationSuperSuper  where value = 'val12'")) {

      Assert.assertFalse(result.hasNext());
    }
  }

  public void testIndexWithNumberProperties() {
    SchemaClass oclass = db.getMetadata().getSchema()
        .createClass("SchemaIndexTest_numberclass");
    oclass.createProperty(db, "1", PropertyType.STRING).setMandatory(db, false);
    oclass.createProperty(db, "2", PropertyType.STRING).setMandatory(db, false);
    oclass.createIndex(db, "SchemaIndexTest_numberclass_1_2", SchemaClass.INDEX_TYPE.UNIQUE,
        "1",
        "2");

    db.getMetadata().getSchema().dropClass(oclass.getName());
  }

  private void assertContains(int[] clusterIds, int clusterId) {
    boolean contains = false;
    for (int cluster : clusterIds) {
      if (cluster == clusterId) {
        contains = true;
        break;
      }
    }

    Assert.assertTrue(contains);
  }

  private void assertDoesNotContain(int[] clusterIds, int clusterId) {
    boolean contains = false;
    for (int cluster : clusterIds) {
      if (cluster == clusterId) {
        contains = true;
        break;
      }
    }

    Assert.assertFalse(contains);
  }
}
