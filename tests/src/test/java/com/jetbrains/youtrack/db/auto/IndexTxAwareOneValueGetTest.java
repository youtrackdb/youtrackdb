package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexTxAwareOneValueGetTest extends BaseDBTest {

  private static final String CLASS_NAME = "idxTxAwareOneValueGetTest";
  private static final String PROPERTY_NAME = "value";
  private static final String INDEX = "idxTxAwareOneValueGetTestIndex";

  @Parameters(value = "remote")
  public IndexTxAwareOneValueGetTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    var cls = db.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(db, PROPERTY_NAME, PropertyType.INTEGER);
    cls.createIndex(db, INDEX, SchemaClass.INDEX_TYPE.UNIQUE, PROPERTY_NAME);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    db.getMetadata().getSchema().getClassInternal(CLASS_NAME).truncate(db);

    super.afterMethod();
  }

  @Test
  public void testPut() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (var stream = index.getInternal().getRids(db, 2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    db.begin();

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 3).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 3)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (var stream = index.getInternal().getRids(db, 2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (var stream = index.getInternal().getRids(db, 3)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test
  public void testRemove() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    var document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    document.save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (var stream = index.getInternal().getRids(db, 2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    db.begin();

    document = db.bindToSession(document);
    document.delete();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (var stream = index.getInternal().getRids(db, 2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (var stream = index.getInternal().getRids(db, 2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  @Test
  public void testRemoveAndPut() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    var document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    document.save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (var stream = index.getInternal().getRids(db, 2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    db.begin();

    document = db.bindToSession(document);
    document.removeField(PROPERTY_NAME);
    document.save();

    document.field(PROPERTY_NAME, 1);
    document.save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (var stream = index.getInternal().getRids(db, 2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    db.rollback();
  }

  @Test
  public void testMultiPut() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    var document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    document.save();
    document.field(PROPERTY_NAME, 0);
    document.field(PROPERTY_NAME, 1);
    document.save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    db.commit();

    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  @Test
  public void testPutAfterTransaction() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    db.commit();

    db.begin();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();
    db.commit();

    try (var stream = index.getInternal().getRids(db, 2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    var document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    document.save();
    document.delete();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    db.commit();

    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test
  public void testPutAfterRemove() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    var document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    document.save();

    document.removeField(PROPERTY_NAME);
    document.save();

    document.field(PROPERTY_NAME, 1).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    db.commit();

    try (var stream = index.getInternal().getRids(db, 1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  public void testInsertionDeletionInsideTx() {
    final var className = "_" + IndexTxAwareOneValueGetTest.class.getSimpleName();
    db.command("create class " + className + " extends V").close();
    db.command("create property " + className + ".name STRING").close();
    db.command("CREATE INDEX " + className + ".name UNIQUE").close();

    db
        .execute(
            "SQL",
            "begin;\n"
                + "insert into "
                + className
                + "(name) values ('c');\n"
                + "let top = (select from "
                + className
                + " where name='c');\n"
                + "delete vertex $top;\n"
                + "commit;\n"
                + "return $top")
        .close();

    try (final var resultSet = db.query("select * from " + className)) {
      try (var stream = resultSet.stream()) {
        Assert.assertEquals(stream.count(), 0);
      }
    }
  }
}
