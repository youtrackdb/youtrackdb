package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexTxAwareOneValueGetValuesTest extends BaseDBTest {

  private static final String CLASS_NAME = "IndexTxAwareOneValueGetValuesTest";
  private static final String FIELD_NAME = "value";
  private static final String INDEX_NAME = "IndexTxAwareOneValueGetValuesTest";

  @Parameters(value = "remote")
  public IndexTxAwareOneValueGetValuesTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = db.getMetadata().getSchema();
    final var cls = schema.createClass(CLASS_NAME);
    cls.createProperty(db, FIELD_NAME, PropertyType.INTEGER);
    cls.createIndex(db, INDEX_NAME, SchemaClass.INDEX_TYPE.UNIQUE, FIELD_NAME);
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    var cls = db.getMetadata().getSchema().getClassInternal(CLASS_NAME);
    cls.truncate(db);
  }

  @Test
  public void testPut() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();
    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));

    Set<Identifiable> resultOne = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    db.begin();

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 3).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));

    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2, 3), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 3);

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemove() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    Set<Identifiable> resultOne = new HashSet<>();
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    db.begin();

    try (var rids = index.getInternal().getRids(db, 1)) {
      rids.map(rid -> rid.getRecord(db)).forEach(record -> ((EntityImpl) record).delete());
    }

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    Set<Identifiable> resultTwo = new HashSet<>();
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemoveAndPut() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultOne = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    db.begin();

    try (var ridStream = index.getInternal().getRids(db, 1)) {
      ridStream.map(rid -> rid.getRecord(db)).forEach(record -> ((EntityImpl) record).delete());
    }
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    db.rollback();
  }

  @Test
  public void testMultiPut() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    final var document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1);
    document.save();
    document.field(FIELD_NAME, 0);
    document.field(FIELD_NAME, 1);
    document.save();

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);

    db.commit();

    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testPutAfterTransaction() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));

    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);
    db.commit();

    db.begin();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 3).save();
    db.commit();

    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2, 3), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 3);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    try (var ridStream = index.getInternal().getRids(db, 1)) {
      ridStream.map(rid -> rid.getRecord(db)).forEach(record -> ((EntityImpl) record).delete());
    }

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);

    db.commit();

    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testRemoveAllWithinTransaction() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    try (var ridStream = index.getInternal().getRids(db, 1)) {
      ridStream.map(rid -> rid.getRecord(db)).forEach(record -> ((EntityImpl) record).delete());
    }

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);

    db.commit();

    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPutAfterRemove() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX_NAME);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 2).save();

    try (var ridStream = index.getInternal().getRids(db, 1)) {
      ridStream.map(rid -> rid.getRecord(db)).forEach(record -> ((EntityImpl) record).delete());
    }
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);

    db.commit();

    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
  }

  private static void streamToSet(
      Stream<RawPair<Object, RID>> stream, Set<Identifiable> result) {
    result.clear();
    result.addAll(stream.map((entry) -> entry.second).collect(Collectors.toSet()));
  }
}
