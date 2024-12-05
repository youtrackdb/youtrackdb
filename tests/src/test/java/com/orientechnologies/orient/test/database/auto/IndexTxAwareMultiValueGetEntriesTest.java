package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
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

    final YTClass cls = database.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(database, FIELD_NAME, YTType.INTEGER);
    cls.createIndex(database, INDEX_NAME, YTClass.INDEX_TYPE.NOTUNIQUE, FIELD_NAME);
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
    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> resultOne = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 4);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> resultThree = new HashSet<>();
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
    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    YTEntityImpl docOne = new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docOne.save();
    YTEntityImpl docTwo = new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docTwo.save();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> resultOne = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    docOne = database.bindToSession(docOne);
    docTwo = database.bindToSession(docTwo);

    docOne.delete();
    docTwo.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> resultThree = new HashSet<>();
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
    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    YTEntityImpl docOne = new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docOne.save();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> resultOne = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    docOne = database.bindToSession(docOne);
    docOne.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> resultThree = new HashSet<>();
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

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    database.begin();
    final YTEntityImpl document = new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    document.save();
    document.field(FIELD_NAME, 0);
    document.field(FIELD_NAME, 1);
    document.save();

    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();
    database.commit();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Set<YTIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
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

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<YTIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
    database.commit();

    database.begin();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();
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

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    YTEntityImpl doc = new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    doc.save();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    doc.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    database.commit();

    Set<YTIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
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

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    YTEntityImpl docOne = new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docOne.save();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    docOne.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Set<YTIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
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

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    final YTEntityImpl docOne = new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1);
    docOne.save();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 2).save();

    docOne.delete();
    new YTEntityImpl(CLASS_NAME).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Set<YTIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);
  }

  private static void streamToSet(
      Stream<ORawPair<Object, YTRID>> stream, Set<YTIdentifiable> result) {
    result.clear();
    result.addAll(stream.map((entry) -> entry.second).collect(Collectors.toSet()));
  }
}
