package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexTxAwareMultiValueGetEntriesTest extends DocumentDBBaseTest {

  private static final String CLASS_NAME = "IndexTxAwareMultiValueGetEntriesTest";
  private static final String FIELD_NAME = "values";
  private static final String INDEX_NAME = "IndexTxAwareMultiValueGetEntriesTestIndex";

  @Parameters(value = "remote")
  public IndexTxAwareMultiValueGetEntriesTest(@Optional Boolean remote) {
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
    database.getMetadata().getSchema().getClass(CLASS_NAME).truncate(database);

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
    Set<Identifiable> resultOne = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 4);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
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
    Set<Identifiable> resultOne = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    docOne = database.bindToSession(docOne);
    docTwo = database.bindToSession(docTwo);

    docOne.delete();
    docTwo.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testRemoveOne() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    EntityImpl docOne = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docOne.save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultOne = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    docOne = database.bindToSession(docOne);
    docOne.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testMultiPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    database.begin();
    final EntityImpl document = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    document.save();
    document.field(FIELD_NAME, 0);
    document.field(FIELD_NAME, 1);
    document.save();

    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();
    database.commit();

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
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    database.commit();

    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
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

    EntityImpl doc = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    doc.save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    doc.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    database.commit();

    Set<Identifiable> result = new HashSet<>();
    Stream<RawPair<Object, RID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 1);

    result = new HashSet<>();
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

    EntityImpl docOne = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docOne.save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    docOne.delete();

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

    final EntityImpl docOne = new EntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docOne.save();
    new EntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    docOne.delete();
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
