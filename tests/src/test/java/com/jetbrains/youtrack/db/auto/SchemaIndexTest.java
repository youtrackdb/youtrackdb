package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SchemaIndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SchemaIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
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


  @Test
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
}
