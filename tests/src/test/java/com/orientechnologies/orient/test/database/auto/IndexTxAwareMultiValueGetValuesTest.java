package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
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
public class IndexTxAwareMultiValueGetValuesTest extends DocumentDBBaseTest {

  private static final String CLASS_NAME = "idxTxAwareMultiValueGetValuesTest";
  private static final String PROPERTY_NAME = "value";
  private static final String INDEX = "idxTxAwareMultiValueGetValuesTestIndex";

  @Parameters(value = "remote")
  public IndexTxAwareMultiValueGetValuesTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final YTClass cls = database.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(database, PROPERTY_NAME, YTType.INTEGER);
    cls.createIndex(database, INDEX, YTClass.INDEX_TYPE.NOTUNIQUE, PROPERTY_NAME);
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
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<YTIdentifiable> resultOne = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);

    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<YTIdentifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 4);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
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
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    EntityImpl documentOne = new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1);
    documentOne.save();
    EntityImpl documentTwo = new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1);
    documentTwo.save();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<YTIdentifiable> resultOne = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    documentOne = database.bindToSession(documentOne);
    documentTwo = database.bindToSession(documentTwo);

    documentOne.delete();
    documentTwo.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<YTIdentifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
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
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    EntityImpl documentOne = new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1);
    documentOne.save();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 2).save();
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<YTIdentifiable> resultOne = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    documentOne = database.bindToSession(documentOne);
    documentOne.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<YTIdentifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
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

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    final EntityImpl document = new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1);
    document.save();

    document.field(PROPERTY_NAME, 0);
    document.field(PROPERTY_NAME, 1);
    document.save();

    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
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

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<YTIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, YTRID>> stream =
        index.getInternal().streamEntries(database, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
    database.commit();

    database.begin();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1).save();
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

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    EntityImpl document = new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1);
    document.save();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
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
  public void testRemoveAllWithinTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    EntityImpl document = new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1);
    document.save();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
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

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    EntityImpl document = new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 1);
    document.save();
    new EntityImpl(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    document.removeField(PROPERTY_NAME);
    document.save();

    document.field(PROPERTY_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
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
