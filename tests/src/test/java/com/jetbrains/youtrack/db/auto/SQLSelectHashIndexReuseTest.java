package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 16.07.13
 */
@Test
@Ignore("Rewrite these tests for the new SQL engine")
public class SQLSelectHashIndexReuseTest extends AbstractIndexReuseTest {

  @Parameters(value = "remote")
  public SQLSelectHashIndexReuseTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.createClass("sqlSelectHashIndexReuseTestClass");

    oClass.createProperty(database, "prop1", PropertyType.INTEGER);
    oClass.createProperty(database, "prop2", PropertyType.INTEGER);
    oClass.createProperty(database, "prop3", PropertyType.INTEGER);
    oClass.createProperty(database, "prop4", PropertyType.INTEGER);
    oClass.createProperty(database, "prop5", PropertyType.INTEGER);
    oClass.createProperty(database, "prop6", PropertyType.INTEGER);
    oClass.createProperty(database, "prop7", PropertyType.STRING);
    oClass.createProperty(database, "prop8", PropertyType.INTEGER);
    oClass.createProperty(database, "prop9", PropertyType.INTEGER);

    oClass.createProperty(database, "fEmbeddedMap", PropertyType.EMBEDDEDMAP, PropertyType.INTEGER);
    oClass.createProperty(database, "fEmbeddedMapTwo", PropertyType.EMBEDDEDMAP,
        PropertyType.INTEGER);

    oClass.createProperty(database, "fLinkMap", PropertyType.LINKMAP);

    oClass.createProperty(database, "fEmbeddedList", PropertyType.EMBEDDEDLIST,
        PropertyType.INTEGER);
    oClass.createProperty(database, "fEmbeddedListTwo", PropertyType.EMBEDDEDLIST,
        PropertyType.INTEGER);

    oClass.createProperty(database, "fLinkList", PropertyType.LINKLIST);

    oClass.createProperty(database, "fEmbeddedSet", PropertyType.EMBEDDEDSET, PropertyType.INTEGER);
    oClass.createProperty(database, "fEmbeddedSetTwo", PropertyType.EMBEDDEDSET,
        PropertyType.INTEGER);

    oClass.createIndex(database, "indexone", SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "prop1",
        "prop2");
    oClass.createIndex(database, "indextwo", SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "prop3");
    oClass.createIndex(database,
        "indexthree", SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "prop1", "prop2", "prop4");
    oClass.createIndex(database,
        "indexfour", SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "prop4", "prop1", "prop3");
    oClass.createIndex(database,
        "indexfive", SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "prop6", "prop1", "prop3");

    oClass.createIndex(database,
        "sqlSelectHashIndexReuseTestEmbeddedMapByKey",
        SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "fEmbeddedMap");
    oClass.createIndex(database,
        "sqlSelectHashIndexReuseTestEmbeddedMapByValue",
        SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "fEmbeddedMap by value");
    oClass.createIndex(database,
        "sqlSelectHashIndexReuseTestEmbeddedList",
        SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "fEmbeddedList");

    oClass.createIndex(database,
        "sqlSelectHashIndexReuseTestEmbeddedMapByKeyProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX,
        "fEmbeddedMapTwo", "prop8");
    oClass.createIndex(database,
        "sqlSelectHashIndexReuseTestEmbeddedMapByValueProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX,
        "fEmbeddedMapTwo by value", "prop8");

    oClass.createIndex(database,
        "sqlSelectHashIndexReuseTestEmbeddedSetProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX,
        "fEmbeddedSetTwo", "prop8");
    oClass.createIndex(database,
        "sqlSelectHashIndexReuseTestProp9EmbeddedSetProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX,
        "prop9",
        "fEmbeddedSetTwo", "prop8");

    oClass.createIndex(database,
        "sqlSelectHashIndexReuseTestEmbeddedListTwoProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX,
        "fEmbeddedListTwo", "prop8");

    final String[] fullTextIndexStrings = {
        "Alice : What is the use of a book, without pictures or conversations?",
        "Rabbit : Oh my ears and whiskers, how late it's getting!",
        "Alice : If it had grown up, it would have made a dreadfully ugly child; but it makes rather"
            + " a handsome pig, I think",
        "The Cat : We're all mad here.",
        "The Hatter : Why is a raven like a writing desk?",
        "The Hatter : Twinkle, twinkle, little bat! How I wonder what you're at.",
        "The Queen : Off with her head!",
        "The Duchess : Tut, tut, child! Everything's got a moral, if only you can find it.",
        "The Duchess : Take care of the sense, and the sounds will take care of themselves.",
        "The King : Begin at the beginning and go on till you come to the end: then stop."
    };

    for (int i = 0; i < 10; i++) {
      final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

      embeddedMap.put("key" + (i * 10 + 1), i * 10 + 1);
      embeddedMap.put("key" + (i * 10 + 2), i * 10 + 2);
      embeddedMap.put("key" + (i * 10 + 3), i * 10 + 3);
      embeddedMap.put("key" + (i * 10 + 4), i * 10 + 1);

      final List<Integer> embeddedList = new ArrayList<Integer>(3);
      embeddedList.add(i * 3);
      embeddedList.add(i * 3 + 1);
      embeddedList.add(i * 3 + 2);

      final Set<Integer> embeddedSet = new HashSet<Integer>();
      embeddedSet.add(i * 10);
      embeddedSet.add(i * 10 + 1);
      embeddedSet.add(i * 10 + 2);

      for (int j = 0; j < 10; j++) {
        database.begin();
        final EntityImpl document = new EntityImpl("sqlSelectHashIndexReuseTestClass");
        document.field("prop1", i);
        document.field("prop2", j);
        document.field("prop3", i * 10 + j);

        document.field("prop4", i);
        document.field("prop5", i);

        document.field("prop6", j);

        document.field("prop7", fullTextIndexStrings[i]);

        document.field("prop8", j);

        document.field("prop9", j % 2);

        document.field("fEmbeddedMap", embeddedMap);
        document.field("fEmbeddedMapTwo", embeddedMap);

        document.field("fEmbeddedList", embeddedList);
        document.field("fEmbeddedListTwo", embeddedList);

        document.field("fEmbeddedSet", embeddedSet);
        document.field("fEmbeddedSetTwo", embeddedSet);

        document.save();
        database.commit();
      }
    }
  }

  @AfterClass
  @Override
  public void afterClass() throws Exception {
    if (database.isClosed()) {
      database = createSessionInstance();
    }

    database.command("drop class sqlSelectHashIndexReuseTestClass").close();

    super.afterClass();
  }

  @Test
  public void testCompositeSearchEquals() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop2 = 2"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
  }

  @Test
  public void testCompositeSearchHasChainOperatorsEquals() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1.asInteger() = 1 and"
                        + " prop2 = 2"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 0);
  }

  @Test
  public void testCompositeSearchEqualsOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed21 == -1) {
      oldcompositeIndexUsed21 = 0;
    }

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    long indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long compositeIndexUsage = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long compositeIndexUsage2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long compositeIndexUsage21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (indexUsage < 0) {
      indexUsage = 0;
    }
    if (compositeIndexUsage < 0) {
      compositeIndexUsage = 0;
    }
    if (compositeIndexUsage2 < 0) {
      compositeIndexUsage2 = 0;
    }
    if (compositeIndexUsage21 < 0) {
      compositeIndexUsage21 = 0;
    }

    Assert.assertEquals(indexUsage, oldIndexUsage);
    Assert.assertEquals(compositeIndexUsage, oldcompositeIndexUsed);
    Assert.assertEquals(compositeIndexUsage2, oldcompositeIndexUsed2);
    Assert.assertEquals(compositeIndexUsage21, oldcompositeIndexUsed21);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldMapIndexByKey() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }

    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed21 == -1) {
      oldcompositeIndexUsed21 = 0;
    }

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where fEmbeddedMapTwo"
                        + " containsKey 'key11'"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop8", 1);
      document.field("fEmbeddedMapTwo", embeddedMap);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    long indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long compositeIndexUsage = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long compositeIndexUsage2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long compositeIndexUsage21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (indexUsage < 0) {
      indexUsage = 0;
    }
    if (compositeIndexUsage < 0) {
      compositeIndexUsage = 0;
    }
    if (compositeIndexUsage2 < 0) {
      compositeIndexUsage2 = 0;
    }
    if (compositeIndexUsage21 < 0) {
      compositeIndexUsage21 = 0;
    }

    Assert.assertEquals(indexUsage, oldIndexUsage);
    Assert.assertEquals(compositeIndexUsage, oldcompositeIndexUsed);
    Assert.assertEquals(compositeIndexUsage2, oldcompositeIndexUsed2);
    Assert.assertEquals(compositeIndexUsage21, oldcompositeIndexUsed21);
  }

  private int containsDocument(final List<EntityImpl> docList, final EntityImpl document) {
    int count = 0;
    for (final EntityImpl docItem : docList) {
      boolean containsAllFields = true;
      for (final String fieldName : document.fieldNames()) {
        if (!document.field(fieldName).equals(docItem.field(fieldName))) {
          containsAllFields = false;
          break;
        }
      }
      if (containsAllFields) {
        count++;
      }
    }
    return count;
  }

  @Test
  public void testCompositeSearchEqualsMapIndexByKey() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed22 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass "
                        + "where prop8 = 1 and fEmbeddedMapTwo containsKey 'key11'"))
            .execute(database);

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = new EntityImpl();
    document.field("prop8", 1);
    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 1);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.2"), oldcompositeIndexUsed22, 1);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldMapIndexByValue() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }
    if (oldcompositeIndexUsed21 == -1) {
      oldcompositeIndexUsed21 = 0;
    }

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where fEmbeddedMapTwo"
                        + " containsValue 22"))
            .execute(database);

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key21", 21);
    embeddedMap.put("key22", 22);
    embeddedMap.put("key23", 23);
    embeddedMap.put("key24", 21);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop8", i);
      document.field("fEmbeddedMapTwo", embeddedMap);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    long indexUsed = profiler.getCounter("db.demo.query.indexUsed");
    long compositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long compositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long compositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (indexUsed < 0) {
      indexUsed = 0;
    }
    if (compositeIndexUsed < 0) {
      compositeIndexUsed = 0;
    }
    if (compositeIndexUsed2 < 0) {
      compositeIndexUsed2 = 0;
    }
    if (compositeIndexUsed21 < 0) {
      compositeIndexUsed21 = 0;
    }

    Assert.assertEquals(indexUsed, oldIndexUsage);
    Assert.assertEquals(compositeIndexUsed, oldcompositeIndexUsed);
    Assert.assertEquals(compositeIndexUsed2, oldcompositeIndexUsed2);
    Assert.assertEquals(compositeIndexUsed21, oldcompositeIndexUsed21);
  }

  @Test
  public void testCompositeSearchEqualsMapIndexByValue() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed22 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass "
                        + "where prop8 = 1 and fEmbeddedMapTwo containsValue 22"))
            .execute(database);

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key21", 21);
    embeddedMap.put("key22", 22);
    embeddedMap.put("key23", 23);
    embeddedMap.put("key24", 21);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = new EntityImpl();
    document.field("prop8", 1);
    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 1);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.2"), oldcompositeIndexUsed22, 1);
  }

  @Test
  public void testCompositeSearchEqualsEmbeddedSetIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed22 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass "
                        + "where prop8 = 1 and fEmbeddedSetTwo contains 12"))
            .execute(database);

    final Set<Integer> embeddedSet = new HashSet<Integer>();
    embeddedSet.add(10);
    embeddedSet.add(11);
    embeddedSet.add(12);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = new EntityImpl();
    document.field("prop8", 1);
    document.field("fEmbeddedSet", embeddedSet);

    Assert.assertEquals(containsDocument(result, document), 1);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.2"), oldcompositeIndexUsed22, 1);
  }

  @Test
  public void testCompositeSearchEqualsEmbeddedSetInMiddleIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }

    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }

    if (oldcompositeIndexUsed33 == -1) {
      oldcompositeIndexUsed33 = 0;
    }

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass "
                        + "where prop9 = 0 and fEmbeddedSetTwo contains 92 and prop8 > 2"))
            .execute(database);

    final Set<Integer> embeddedSet = new HashSet<Integer>(3);
    embeddedSet.add(90);
    embeddedSet.add(91);
    embeddedSet.add(92);

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i < 3; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop8", i * 2 + 4);
      document.field("prop9", 0);
      document.field("fEmbeddedSet", embeddedSet);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    long indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long compositeIndexUsage = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long compositeIndexUsage2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long compositeIndexUsage3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long compositeIndexUsage33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    if (indexUsage < 0) {
      indexUsage = 0;
    }
    if (compositeIndexUsage < 0) {
      compositeIndexUsage = 0;
    }
    if (compositeIndexUsage2 < 0) {
      compositeIndexUsage2 = 0;
    }
    if (compositeIndexUsage3 < 0) {
      compositeIndexUsage3 = 0;
    }
    if (compositeIndexUsage33 < 0) {
      compositeIndexUsage33 = 0;
    }

    Assert.assertEquals(indexUsage, oldIndexUsage);
    Assert.assertEquals(compositeIndexUsage, oldcompositeIndexUsed);
    Assert.assertEquals(compositeIndexUsage2, oldcompositeIndexUsed2);
    Assert.assertEquals(compositeIndexUsage3, oldcompositeIndexUsed3);
    Assert.assertEquals(compositeIndexUsage33, oldcompositeIndexUsed33);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldEmbeddedListIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }

    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed21 == -1) {
      oldcompositeIndexUsed21 = 0;
    }

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where fEmbeddedListTwo contains"
                        + " 4"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    final List<Integer> embeddedList = new ArrayList<Integer>(3);
    embeddedList.add(3);
    embeddedList.add(4);
    embeddedList.add(5);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop8", i);
      document.field("fEmbeddedListTwo", embeddedList);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    long indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long compositeIndexUsage = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long compositeIndexUsage2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long compositeIndexUsage21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (indexUsage < 0) {
      indexUsage = 0;
    }
    if (compositeIndexUsage < 0) {
      compositeIndexUsage = 0;
    }
    if (compositeIndexUsage2 < 0) {
      compositeIndexUsage2 = 0;
    }
    if (compositeIndexUsage21 < 0) {
      compositeIndexUsage21 = 0;
    }

    Assert.assertEquals(indexUsage, oldIndexUsage);
    Assert.assertEquals(compositeIndexUsage, oldcompositeIndexUsed);
    Assert.assertEquals(compositeIndexUsage2, oldcompositeIndexUsed2);
    Assert.assertEquals(compositeIndexUsage21, oldcompositeIndexUsed21);
  }

  @Test
  public void testCompositeSearchEqualsEmbeddedListIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed22 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where"
                        + " prop8 = 1 and fEmbeddedListTwo contains 4"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final List<Integer> embeddedList = new ArrayList<Integer>(3);
    embeddedList.add(3);
    embeddedList.add(4);
    embeddedList.add(5);

    final EntityImpl document = new EntityImpl();
    document.field("prop8", 1);
    document.field("fEmbeddedListTwo", embeddedList);

    Assert.assertEquals(containsDocument(result, document), 1);
    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.2"), oldcompositeIndexUsed22, 1);
  }

  @Test
  public void testNoCompositeSearchEquals() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 = 1"))
            .execute(database);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 0);
    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", i);
      document.field("prop2", 1);

      Assert.assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchEqualsWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = ? and prop2 = ?"))
            .execute(database, 1, 2);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = ?"))
            .execute(database, 1);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testNoCompositeSearchEqualsWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 = ?"))
            .execute(database, 1);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", i);
      document.field("prop2", 1);

      Assert.assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchGT() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop2 > 2"))
            .execute(database);

    Assert.assertEquals(result.size(), 7);

    for (int i = 3; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchGTOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 > 7"))
            .execute(database);

    Assert.assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchGTOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 > 7"))
            .execute(database);

    Assert.assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchGTWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = ? and prop2 > ?"))
            .execute(database, 1, 2);

    Assert.assertEquals(result.size(), 7);

    for (int i = 3; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchGTOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 > ?"))
            .execute(database, 7);

    Assert.assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchGTOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 > ?"))
            .execute(database, 7);

    Assert.assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchGTQ() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop2 >="
                        + " 2"))
            .execute(database);

    Assert.assertEquals(result.size(), 8);

    for (int i = 2; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchGTQOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 >= 7"))
            .execute(database);

    Assert.assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchGTQOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 >= 7"))
            .execute(database);

    Assert.assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchGTQWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = ? and prop2 >="
                        + " ?"))
            .execute(database, 1, 2);

    Assert.assertEquals(result.size(), 8);

    for (int i = 2; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchGTQOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 >= ?"))
            .execute(database, 7);

    Assert.assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchGTQOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 >= ?"))
            .execute(database, 7);

    Assert.assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchLTQ() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop2 <="
                        + " 2"))
            .execute(database);

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i <= 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchLTQOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 <= 7"))
            .execute(database);

    Assert.assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchLTQOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 <= 7"))
            .execute(database);

    Assert.assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchLTQWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = ? and prop2 <="
                        + " ?"))
            .execute(database, 1, 2);

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i <= 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchLTQOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 <= ?"))
            .execute(database, 7);

    Assert.assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchLTQOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 <= ?"))
            .execute(database, 7);

    Assert.assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchLT() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop2 < 2"))
            .execute(database);

    Assert.assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchLTOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 < 7"))
            .execute(database);

    Assert.assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchLTOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 < 7"))
            .execute(database);

    Assert.assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchLTWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = ? and prop2 < ?"))
            .execute(database, 1, 2);

    Assert.assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchLTOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 < ?"))
            .execute(database, 7);

    Assert.assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchLTOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 < ?"))
            .execute(database, 7);

    Assert.assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchBetween() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop2"
                        + " between 1 and 3"))
            .execute(database);

    Assert.assertEquals(result.size(), 3);

    for (int i = 1; i <= 3; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchBetweenOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 between 1 and 3"))
            .execute(database);

    Assert.assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchBetweenOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 between 1 and 3"))
            .execute(database);

    Assert.assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchBetweenWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop2"
                        + " between ? and ?"))
            .execute(database, 1, 3);

    Assert.assertEquals(result.size(), 3);

    for (int i = 1; i <= 3; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchBetweenOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 between ? and ?"))
            .execute(database, 1, 3);

    Assert.assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testCompositeSearchBetweenOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop2 between ? and ?"))
            .execute(database, 1, 3);

    Assert.assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testSingleSearchEquals() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 = 1"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 1);
    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchEqualsWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 = ?"))
            .execute(database, 1);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 1);
    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchGT() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 > 90"))
            .execute(database);

    Assert.assertEquals(result.size(), 9);

    for (int i = 91; i < 100; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchGTWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 > ?"))
            .execute(database, 90);

    Assert.assertEquals(result.size(), 9);

    for (int i = 91; i < 100; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  private void assertProfileCount(long newProfilerValue, long oldProfilerValue) {
    assertProfileCount(newProfilerValue, oldProfilerValue, 0);
  }

  private void assertProfileCount(long newProfilerValue, long oldProfilerValue, long diff) {
    if (oldProfilerValue == -1) {
      if (diff == 0) {
        Assert.assertTrue(newProfilerValue == -1 || newProfilerValue == 0);
      } else {
        Assert.assertEquals(newProfilerValue, diff);
      }
    } else {
      Assert.assertEquals(newProfilerValue, oldProfilerValue + diff);
    }
  }

  @Test
  public void testSingleSearchGTQ() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 >= 90"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    for (int i = 90; i < 100; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchGTQWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 >= ?"))
            .execute(database, 90);

    Assert.assertEquals(result.size(), 10);

    for (int i = 90; i < 100; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchLTQ() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 <= 10"))
            .execute(database);

    Assert.assertEquals(result.size(), 11);

    for (int i = 0; i <= 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchLTQWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 <= ?"))
            .execute(database, 10);

    Assert.assertEquals(result.size(), 11);

    for (int i = 0; i <= 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchLT() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 < 10"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchLTWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 < ?"))
            .execute(database, 10);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchBetween() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 between 1 and 10"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    for (int i = 1; i <= 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchBetweenWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 between ? and ?"))
            .execute(database, 1, 10);

    Assert.assertEquals(result.size(), 10);

    for (int i = 1; i <= 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchIN() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 in [0, 5, 10]"))
            .execute(database);

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i <= 10; i += 5) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchINWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop3 in [?, ?, ?]"))
            .execute(database, 0, 5, 10);

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i <= 10; i += 5) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testMostSpecificOnesProcessedFirst() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where (prop1 = 1 and prop2 = 1)"
                        + " and prop3 = 11"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 11);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
  }

  @Test
  public void testTripleSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop2 = 1"
                        + " and prop4 = 1"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3"), oldcompositeIndexUsed3, 1);
  }

  @Test
  public void testTripleSearchLastFieldNotInIndexFirstCase() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where (prop1 = 1 and prop2 = 1)"
                        + " and prop5 >= 1"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop5").intValue(), 1);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
  }

  @Test
  public void testTripleSearchLastFieldNotInIndexSecondCase() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop4 >="
                        + " 1"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);
      document.field("prop4", 1);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    long newIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    assertProfileCount(newIndexUsage, oldIndexUsage);

    long newcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    assertProfileCount(newcompositeIndexUsed, oldcompositeIndexUsed);

    long newcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    assertProfileCount(newcompositeIndexUsed2, oldcompositeIndexUsed2);
  }

  @Test
  public void testTripleSearchLastFieldInIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop4 = 1"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);
      document.field("prop4", 1);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    long newIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    assertProfileCount(newIndexUsage, oldIndexUsage);

    long newcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    assertProfileCount(newcompositeIndexUsed, oldcompositeIndexUsed);

    long newcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    assertProfileCount(newcompositeIndexUsed3, oldcompositeIndexUsed3);
  }

  @Test
  public void testTripleSearchLastFieldsCanNotBeMerged() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop6 <= 1 and prop4 <"
                        + " 1"))
            .execute(database);

    Assert.assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop6", i);
      document.field("prop4", 0);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    long indexUsed = profiler.getCounter("db.demo.query.indexUsed");
    long compositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long compositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");

    if (indexUsed < 0) {
      indexUsed = 0;
    }

    if (compositeIndexUsed < 0) {
      compositeIndexUsed = 0;
    }

    if (compositeIndexUsed3 < 0) {
      compositeIndexUsed3 = 0;
    }

    Assert.assertEquals(indexUsed, oldIndexUsage);
    Assert.assertEquals(compositeIndexUsed, oldcompositeIndexUsed);
    Assert.assertEquals(compositeIndexUsed3, oldcompositeIndexUsed3);
  }

  @Test
  public void testLastFieldNotCompatibleOperator() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1 and prop2 + 1 ="
                        + " 3"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testEmbeddedMapByKeyIndexReuse() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where fEmbeddedMap containskey"
                        + " 'key12'"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    final EntityImpl document = new EntityImpl();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 10);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testEmbeddedMapBySpecificKeyIndexReuse() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where ( fEmbeddedMap"
                        + " containskey 'key12' ) and ( fEmbeddedMap['key12'] = 12 )"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    final EntityImpl document = new EntityImpl();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 10);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testEmbeddedMapByValueIndexReuse() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where fEmbeddedMap"
                        + " containsvalue 11"))
            .execute(database);

    Assert.assertEquals(result.size(), 10);

    final EntityImpl document = new EntityImpl();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 10);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testEmbeddedListIndexReuse() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where fEmbeddedList contains"
                        + " 7"))
            .execute(database);

    final List<Integer> embeddedList = new ArrayList<Integer>(3);
    embeddedList.add(6);
    embeddedList.add(7);
    embeddedList.add(8);

    final EntityImpl document = new EntityImpl();
    document.field("fEmbeddedList", embeddedList);

    Assert.assertEquals(containsDocument(result, document), 10);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testNotIndexOperatorFirstCase() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where (prop1 = 1 and prop2  ="
                        + " 2) and (prop4 = 3 or prop4 = 1)"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
  }

  @Test
  public void testNotIndexOperatorSecondCase() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where ( prop1 = 1 and prop2 = 2"
                        + " ) or ( prop4  = 1 and prop6 = 2 )"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop6").intValue(), 2);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeIndexEmptyResult() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop1 = 1777 and prop2  ="
                        + " 2777"))
            .execute(database);

    Assert.assertEquals(result.size(), 0);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
  }

  @Test
  public void testReuseOfIndexOnSeveralClassesFields() {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass superClass = schema.createClass("sqlSelectHashIndexReuseTestSuperClass");
    superClass.createProperty(database, "prop0", PropertyType.INTEGER);
    final SchemaClass oClass = schema.createClass("sqlSelectHashIndexReuseTestChildClass",
        superClass);
    oClass.createProperty(database, "prop1", PropertyType.INTEGER);

    oClass.createIndex(database,
        "sqlSelectHashIndexReuseTestOnPropertiesFromClassAndSuperclass",
        SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "prop0", "prop1");

    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    database.begin();
    final EntityImpl docOne = new EntityImpl("sqlSelectHashIndexReuseTestChildClass");
    docOne.field("prop0", 0);
    docOne.field("prop1", 1);
    docOne.save();

    final EntityImpl docTwo = new EntityImpl("sqlSelectHashIndexReuseTestChildClass");
    docTwo.field("prop0", 2);
    docTwo.field("prop1", 3);
    docTwo.save();
    database.commit();

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestChildClass where prop0 = 0 and prop1"
                        + " = 1"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2, 1);
  }

  @Test
  public void testCountFunctionWithNotUniqueIndex() {
    SchemaClass klazz =
        database.getMetadata().getSchema().getOrCreateClass("CountFunctionWithNotUniqueHashIndex");
    if (!klazz.existsProperty("a")) {
      klazz.createProperty(database, "a", PropertyType.STRING);
      klazz.createIndex(database, "CountFunctionWithNotUniqueHashIndex_A", "NOTUNIQUE_HASH_INDEX",
          "a");
    }

    database.begin();
    EntityImpl doc =
        database
            .<EntityImpl>newInstance("CountFunctionWithNotUniqueHashIndex")
            .field("a", "a")
            .field("b", "b");
    doc.save();
    database.commit();

    try (var rs = database.query(
        "select count(*) as count from CountFunctionWithNotUniqueHashIndex where a = 'a' and"
            + " b = 'b'")) {
      if (!remoteDB) {
        Assert.assertEquals(indexesUsed(rs.getExecutionPlan().orElseThrow()), 1);
      }

      Assert.assertEquals(rs.findFirst().<Long>getProperty("count"), 1L);
    }

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();
  }

  @Test
  public void testCountFunctionWithUniqueIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    SchemaClass klazz =
        database.getMetadata().getSchema().getOrCreateClass("CountFunctionWithUniqueHashIndex");
    if (!klazz.existsProperty("a")) {
      klazz.createProperty(database, "a", PropertyType.STRING);
      klazz.createIndex(database, "CountFunctionWithUniqueHashIndex_A", "UNIQUE_HASH_INDEX", "a");
    }

    database.begin();
    EntityImpl doc =
        database
            .<EntityImpl>newInstance("CountFunctionWithUniqueHashIndex")
            .field("a", "a")
            .field("b", "b");
    doc.save();
    database.commit();

    EntityImpl result =
        (EntityImpl)
            database
                .query(
                    new SQLSynchQuery<EntityImpl>(
                        "select count(*) from CountFunctionWithUniqueHashIndex where a = 'a'"))
                .get(0);

    Assert.assertEquals(result.<Object>field("count", Long.class), 1L);
    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();
  }

  @Test
  public void testCompositeSearchIn1() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop4 = 1 and prop1 = 1"
                        + " and prop3 in [13, 113]"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 13);

    assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3"), oldcompositeIndexUsed3, 1);
    assertProfileCount(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3.3"), oldcompositeIndexUsed33, 1);
  }

  @Test
  public void testCompositeSearchIn2() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop4 = 1 and prop1 in"
                        + " [1, 2] and prop3 = 13"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 13);

    // TODO improve query execution plan so that also next statements succeed (in 2.0 it's not
    // guaranteed)
    // assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage , 1);
    // assertProfileCount(profiler.getCounter("db.demo.query.compositeIndexUsed"),
    // oldcompositeIndexUsed , 1);
    // assertProfileCount(profiler.getCounter("db.demo.query.compositeIndexUsed.3"),
    // oldcompositeIndexUsed3 , 1);
    // Assert.assertTrue(profiler.getCounter("db.demo.query.compositeIndexUsed.3.3") <
    // oldcompositeIndexUsed33 + 1);
  }

  @Test
  public void testCompositeSearchIn3() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop4 = 1 and prop1 in"
                        + " [1, 2] and prop3 in [13, 15]"))
            .execute(database);

    Assert.assertEquals(result.size(), 2);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertTrue(
        document.<Integer>field("prop3").equals(13) || document.<Integer>field("prop3").equals(15));

    // TODO improve query execution plan so that also next statements succeed (in 2.0 it's not
    // guaranteed)
    // assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage , 1);
    // assertProfileCount(profiler.getCounter("db.demo.query.compositeIndexUsed"),
    // oldcompositeIndexUsed , 1);
    // assertProfileCount(profiler.getCounter("db.demo.query.compositeIndexUsed.3"),
    // oldcompositeIndexUsed3 , 1);
    // Assert.assertTrue(profiler.getCounter("db.demo.query.compositeIndexUsed.3.3") <
    // oldcompositeIndexUsed33 + 1);
  }

  @Test
  public void testCompositeSearchIn4() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    final List<EntityImpl> result =
        database
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select * from sqlSelectHashIndexReuseTestClass where prop4 in [1, 2] and prop1"
                        + " = 1 and prop3 = 13"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);

    final EntityImpl document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 13);

    // TODO improve query execution plan so that also next statements succeed (in 2.0 it's not
    // guaranteed)
    // assertProfileCount(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage , 1);
    // assertProfileCount(profiler.getCounter("db.demo.query.compositeIndexUsed"),
    // oldcompositeIndexUsed , 1);
    // Assert.assertTrue(profiler.getCounter("db.demo.query.compositeIndexUsed.3") <
    // oldcompositeIndexUsed3 , 1);
    // Assert.assertTrue(profiler.getCounter("db.demo.query.compositeIndexUsed.3.3") <
    // oldcompositeIndexUsed33 + 1);
  }
}
