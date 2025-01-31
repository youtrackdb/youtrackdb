package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexTxAwareMultiValueGetValuesTest extends BaseDBTest {

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

    final var cls = db.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(db, PROPERTY_NAME, PropertyType.INTEGER);
    cls.createIndex(db, INDEX, SchemaClass.INDEX_TYPE.NOTUNIQUE, PROPERTY_NAME);
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
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultOne = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);

    Assert.assertEquals(resultOne.size(), 3);

    db.begin();

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 4);

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testRemove() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    var documentOne = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    documentOne.save();
    var documentTwo = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    documentTwo.save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultOne = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    db.begin();

    documentOne = db.bindToSession(documentOne);
    documentTwo = db.bindToSession(documentTwo);

    documentOne.delete();
    documentTwo.delete();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testRemoveOne() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();
    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    var documentOne = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    documentOne.save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();
    db.commit();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultOne = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    db.begin();

    documentOne = db.bindToSession(documentOne);
    documentOne.delete();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    db.rollback();

    Assert.assertNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testMultiPut() {
    if (db.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    db.begin();

    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    final var document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    document.save();

    document.field(PROPERTY_NAME, 0);
    document.field(PROPERTY_NAME, 1);
    document.save();

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
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

    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1).save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
    db.commit();

    db.begin();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1).save();
    db.commit();

    stream = index.getInternal().streamEntries(db, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 3);
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
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    document.delete();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
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

    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    var document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    document.save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    document.delete();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
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

    final var index = db.getMetadata().getIndexManagerInternal().getIndex(db, INDEX);

    var document = ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);
    document.save();
    ((EntityImpl) db.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2).save();

    document.removeField(PROPERTY_NAME);
    document.save();

    document.field(PROPERTY_NAME, 1).save();

    Assert.assertNotNull(db.getTransaction().getIndexChanges(INDEX));
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
