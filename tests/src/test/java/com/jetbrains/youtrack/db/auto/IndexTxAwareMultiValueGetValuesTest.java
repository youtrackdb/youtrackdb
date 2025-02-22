package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
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

    final var cls = session.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(session, PROPERTY_NAME, PropertyType.INTEGER);
    cls.createIndex(session, INDEX, SchemaClass.INDEX_TYPE.NOTUNIQUE, PROPERTY_NAME);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    session.getMetadata().getSchema().getClassInternal(CLASS_NAME).truncate(session);

    super.afterMethod();
  }

  @Test
  public void testPut() {
    if (session.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    session.begin();
    final var index = session.getMetadata().getIndexManagerInternal().getIndex(session, INDEX);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2);

    session.commit();

    Assert.assertNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultOne = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);

    Assert.assertEquals(resultOne.size(), 3);

    session.begin();

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2);

    Assert.assertNotNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 4);

    session.rollback();

    Assert.assertNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testRemove() {
    if (session.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    session.begin();
    final var index = session.getMetadata().getIndexManagerInternal().getIndex(session, INDEX);

    var documentOne = ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    var documentTwo = ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2);

    session.commit();

    Assert.assertNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultOne = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    session.begin();

    documentOne = session.bindToSession(documentOne);
    documentTwo = session.bindToSession(documentTwo);

    documentOne.delete();
    documentTwo.delete();

    Assert.assertNotNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    session.rollback();

    Assert.assertNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testRemoveOne() {
    if (session.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    session.begin();
    final var index = session.getMetadata().getIndexManagerInternal().getIndex(session, INDEX);

    var documentOne = ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2);

    session.commit();

    Assert.assertNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultOne = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    session.begin();

    documentOne = session.bindToSession(documentOne);
    documentOne.delete();

    Assert.assertNotNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    session.rollback();

    Assert.assertNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testMultiPut() {
    if (session.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    session.begin();

    final var index = session.getMetadata().getIndexManagerInternal().getIndex(session, INDEX);

    final var document = ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    document.field(PROPERTY_NAME, 0);
    document.field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2);

    Assert.assertNotNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);

    session.commit();

    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testPutAfterTransaction() {
    if (session.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    session.begin();

    final var index = session.getMetadata().getIndexManagerInternal().getIndex(session, INDEX);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2);

    Assert.assertNotNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
    session.commit();

    session.begin();
    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    session.commit();

    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 3);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (session.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    session.begin();

    final var index = session.getMetadata().getIndexManagerInternal().getIndex(session, INDEX);

    var document = ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2);

    document.delete();

    Assert.assertNotNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);

    session.commit();

    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testRemoveAllWithinTransaction() {
    if (session.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    session.begin();

    final var index = session.getMetadata().getIndexManagerInternal().getIndex(session, INDEX);

    var document = ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2);

    document.delete();

    Assert.assertNotNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);

    session.commit();

    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPutAfterRemove() {
    if (session.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    session.begin();

    final var index = session.getMetadata().getIndexManagerInternal().getIndex(session, INDEX);

    var document = ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 1);

    ((EntityImpl) session.newEntity(CLASS_NAME)).field(PROPERTY_NAME, 2);

    document.removeField(PROPERTY_NAME);

    document.field(PROPERTY_NAME, 1);

    Assert.assertNotNull(session.getTransaction().getIndexChanges(INDEX));
    Set<Identifiable> result = new HashSet<>();
    var stream =
        index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);

    session.commit();

    stream = index.getInternal().streamEntries(session, Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
  }

  private static void streamToSet(
      Stream<RawPair<Object, RID>> stream, Set<Identifiable> result) {
    result.clear();
    result.addAll(stream.map((entry) -> entry.second).collect(Collectors.toSet()));
  }
}
