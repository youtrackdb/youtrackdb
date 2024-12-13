package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.Schema;
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
public class IndexTxAwareOneValueGetValuesTest extends DocumentDBBaseTest {

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

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass cls = schema.createClass(CLASS_NAME);
    cls.createProperty(database, FIELD_NAME, PropertyType.INTEGER);
    cls.createIndex(database, INDEX_NAME, SchemaClass.INDEX_TYPE.UNIQUE, FIELD_NAME);
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    var cls = database.getMetadata().getSchema().getClassInternal(CLASS_NAME);
    cls.truncate(database);
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
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Set<Identifiable> resultOne = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 3).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2, 3), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 3);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemove() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    Set<Identifiable> resultOne = new HashSet<>();
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    try (Stream<RID> rids = index.getInternal().getRids(database, 1)) {
      rids.map(RID::getRecord).forEach(record -> ((EntityImpl) record).delete());
    }

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    Set<Identifiable> resultTwo = new HashSet<>();
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemoveAndPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultOne = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    try (Stream<RID> ridStream = index.getInternal().getRids(database, 1)) {
      ridStream.map(RID::getRecord).forEach(record -> ((EntityImpl) record).delete());
    }
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();
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
    document.field(FIELD_NAME, 0);
    document.field(FIELD_NAME, 1);
    document.save();

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> result = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
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
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Set<Identifiable> result = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);
    database.commit();

    database.begin();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 3).save();
    database.commit();

    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2, 3), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 3);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    try (Stream<RID> ridStream = index.getInternal().getRids(database, 1)) {
      ridStream.map(RID::getRecord).forEach(record -> ((EntityImpl) record).delete());
    }

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> result = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testRemoveAllWithinTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    try (Stream<RID> ridStream = index.getInternal().getRids(database, 1)) {
      ridStream.map(RID::getRecord).forEach(record -> ((EntityImpl) record).delete());
    }

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> result = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPutAfterRemove() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    try (Stream<RID> ridStream = index.getInternal().getRids(database, 1)) {
      ridStream.map(RID::getRecord).forEach(record -> ((EntityImpl) record).delete());
    }
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> result = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
  }

  private static void streamToSet(
      Stream<RawPair<Object, RID>> stream, Set<Identifiable> result) {
    result.clear();
    result.addAll(stream.map((entry) -> entry.second).collect(Collectors.toSet()));
  }
}
