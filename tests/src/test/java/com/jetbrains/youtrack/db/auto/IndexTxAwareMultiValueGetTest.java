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

    final SchemaClass cls = database.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(database, FIELD_NAME, PropertyType.INTEGER);
    cls.createIndex(database, INDEX_NAME, SchemaClass.INDEX_TYPE.NOTUNIQUE, FIELD_NAME);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.getMetadata().getSchema().getClassInternal(CLASS_NAME).truncate(database);

    super.afterMethod();
  }

  @Test
  public void testPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(database, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.begin();

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 2)) {
      Assert.assertEquals(stream.count(), 2);
    }

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(database, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testRemove() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    EntityImpl docOne = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docOne.save();
    EntityImpl docTwo = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docTwo.save();

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(database, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.begin();

    docOne = database.bindToSession(docOne);
    docTwo = database.bindToSession(docTwo);

    docOne.delete();
    docTwo.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (Stream<RID> stream = index.getInternal().getRids(database, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(database, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testRemoveOne() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    EntityImpl document = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    document.save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(database, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.begin();

    document = database.bindToSession(document);
    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
    try (Stream<RID> stream = index.getInternal().getRids(database, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<RID> stream = index.getInternal().getRids(database, 2)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testMultiPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    final EntityImpl document = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    document.save();
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }

    document.field(FIELD_NAME, 0);
    document.field(FIELD_NAME, 1);
    document.save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
    database.commit();

    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testPutAfterTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
    database.commit();

    database.begin();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    database.commit();

    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 2);
    }
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    final EntityImpl document = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    document.save();
    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    database.commit();

    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 0);
    }
  }

  @Test
  public void testPutAfterRemove() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    final EntityImpl document = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    document.save();
    document.removeField(FIELD_NAME);
    document.save();

    document.field(FIELD_NAME, 1).save();
    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.commit();

    try (Stream<RID> stream = index.getInternal().getRids(database, 1)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }
}
