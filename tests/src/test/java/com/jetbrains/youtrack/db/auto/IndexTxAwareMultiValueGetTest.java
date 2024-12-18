package com.jetbrains.youtrack.db.auto;

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
public class IndexTxAwareMultiValueGetTest extends BaseDBTest {

  private static final String CLASS_NAME = "idxTxAwareMultiValueGetTest";
  private static final String FIELD_NAME = "value";
  private static final String INDEX_NAME = "idxTxAwareMultiValueGetTestIndex";

  @Parameters(value = "remote")
  public IndexTxAwareMultiValueGetTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final SchemaClass cls = db.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(db, FIELD_NAME, PropertyType.INTEGER);
    cls.createIndex(db, INDEX_NAME, SchemaClass.INDEX_TYPE.NOTUNIQUE, FIELD_NAME);
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
    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(db, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    db.begin();

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 2)) {
      Assert.assertEquals(stream.count(), 2);
    }

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(db, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testRemove() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    EntityImpl docOne = ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1);
    docOne.save();
    EntityImpl docTwo = ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1);
    docTwo.save();

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(db, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    db.begin();

    docOne = db.bindToSession(docOne);
    docTwo = db.bindToSession(docTwo);

    docOne.delete();
    docTwo.delete();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = index.getInternal().getRids(db, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(db, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testRemoveOne() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    EntityImpl document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1);
    document.save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(db, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    db.begin();

    document = db.bindToSession(document);
    document.delete();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
    try (Stream<RID> stream = index.getInternal().getRids(db, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(db, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testMultiPut() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    final EntityImpl document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1);
    document.save();
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }

    document.field(FIELD_NAME, 0);
    document.field(FIELD_NAME, 1);
    document.save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
    db.commit();

    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testPutAfterTransaction() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
    db.commit();

    db.begin();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    db.commit();

    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    final EntityImpl document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1);
    document.save();
    document.delete();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    db.commit();

    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 0);
    }
  }

  @Test
  public void testPutAfterRemove() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    final EntityImpl document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1);
    document.save();
    document.removeField(FIELD_NAME);
    document.save();

    document.field(FIELD_NAME, 1).save();
    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }

    db.commit();

    try (Stream<RID> stream = index.getInternal().getRids(db, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }
}
