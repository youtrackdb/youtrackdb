package com.jetbrains.youtrack.db.auto;

import static org.testng.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLSelectIndexReuseTest extends AbstractIndexReuseTest {

  @Parameters(value = "remote")
  public SQLSelectIndexReuseTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.createClass("sqlSelectIndexReuseTestClass");

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

    oClass.createIndex(database, "indexone", SchemaClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");
    oClass.createIndex(database, "indextwo", SchemaClass.INDEX_TYPE.UNIQUE, "prop3");
    oClass.createIndex(database, "indexthree", SchemaClass.INDEX_TYPE.NOTUNIQUE, "prop1", "prop2",
        "prop4");
    oClass.createIndex(database, "indexfour", SchemaClass.INDEX_TYPE.NOTUNIQUE, "prop4", "prop1",
        "prop3");
    oClass.createIndex(database, "indexfive", SchemaClass.INDEX_TYPE.NOTUNIQUE, "prop6", "prop1",
        "prop3");

    oClass.createIndex(database,
        "sqlSelectIndexReuseTestEmbeddedMapByKey", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "fEmbeddedMap");
    oClass.createIndex(database,
        "sqlSelectIndexReuseTestEmbeddedMapByValue",
        SchemaClass.INDEX_TYPE.NOTUNIQUE, "fEmbeddedMap by value");
    oClass.createIndex(database,
        "sqlSelectIndexReuseTestEmbeddedList", SchemaClass.INDEX_TYPE.NOTUNIQUE, "fEmbeddedList");

    oClass.createIndex(database,
        "sqlSelectIndexReuseTestEmbeddedMapByKeyProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "fEmbeddedMapTwo", "prop8");
    oClass.createIndex(database,
        "sqlSelectIndexReuseTestEmbeddedMapByValueProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "fEmbeddedMapTwo by value", "prop8");

    oClass.createIndex(database,
        "sqlSelectIndexReuseTestEmbeddedSetProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "fEmbeddedSetTwo", "prop8");
    oClass.createIndex(database,
        "sqlSelectIndexReuseTestProp9EmbeddedSetProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "prop9",
        "fEmbeddedSetTwo", "prop8");

    oClass.createIndex(database,
        "sqlSelectIndexReuseTestEmbeddedListTwoProp8",
        SchemaClass.INDEX_TYPE.NOTUNIQUE,
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
        final EntityImpl document = new EntityImpl("sqlSelectIndexReuseTestClass");
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
  public void afterClass() throws Exception {
    if (database.isClosed()) {
      database = createSessionInstance();
    }

    database.command("drop class sqlSelectIndexReuseTestClass").close();

    super.afterClass();
  }

  @Test
  public void testCompositeSearchEquals() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 2"
    );

    final Entity document = result.getFirst();
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop2").intValue(), 2);
  }

  @Test
  public void testCompositeSearchHasChainOperatorsEquals() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop1.asInteger() = 1 and prop2 = 2");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop2").intValue(), 2);
  }

  @Test
  public void testCompositeSearchEqualsOneField() {
    final List<Entity> result = tester.queryWithIndex(2, 1,
        "select * from sqlSelectIndexReuseTestClass where prop1 = 1");

    assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchEqualsOneFieldWithLimit() {
    final List<Entity> result = tester.queryWithIndex(2, 1,
        "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop3 = 18 limit 1");

    assertEquals(result.size(), 1);

    final EntityImpl document = new EntityImpl();
    document.field("prop1", 1);
    document.field("prop3", 18);

    assertEquals(containsDocument(result, document), 1);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldMapIndexByKey() {
    final List<Entity> result = tester.queryWithIndex(2, 1,
        "select * from sqlSelectIndexReuseTestClass where fEmbeddedMapTwo containsKey 'key11'");

    assertEquals(result.size(), 10);

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop8", 1);
      document.field("fEmbeddedMapTwo", embeddedMap);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchEqualsMapIndexByKey() {
    final List<Entity> result = tester.queryWithIndex(2, 2,
        "select * from sqlSelectIndexReuseTestClass where prop8 = 1 and fEmbeddedMapTwo containsKey 'key11'");

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    assertEquals(result.size(), 1);

    final EntityImpl document = new EntityImpl();
    document.field("prop8", 1);
    document.field("fEmbeddedMap", embeddedMap);

    assertEquals(containsDocument(result, document), 1);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldMapIndexByValue() {
    final List<Entity> result = tester.queryWithIndex(2, 1,
        "select * from sqlSelectIndexReuseTestClass where fEmbeddedMapTwo containsValue 22");

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key21", 21);
    embeddedMap.put("key22", 22);
    embeddedMap.put("key23", 23);
    embeddedMap.put("key24", 21);

    assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop8", i);
      document.field("fEmbeddedMapTwo", embeddedMap);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchEqualsMapIndexByValue() {
    final List<Entity> result = tester.queryWithIndex(2, 2,
        "select * from sqlSelectIndexReuseTestClass where prop8 = 1 and fEmbeddedMapTwo containsValue 22");

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key21", 21);
    embeddedMap.put("key22", 22);
    embeddedMap.put("key23", 23);
    embeddedMap.put("key24", 21);

    assertEquals(result.size(), 1);

    final EntityImpl document = new EntityImpl();
    document.field("prop8", 1);
    document.field("fEmbeddedMap", embeddedMap);

    assertEquals(containsDocument(result, document), 1);
  }

  @Test
  public void testCompositeSearchEqualsEmbeddedSetIndex() {
    final List<Entity> result = tester.queryWithIndex(2, 2,
        "select * from sqlSelectIndexReuseTestClass where prop8 = 1 and fEmbeddedSetTwo contains 12");

    final Set<Integer> embeddedSet = new HashSet<Integer>();
    embeddedSet.add(10);
    embeddedSet.add(11);
    embeddedSet.add(12);

    assertEquals(result.size(), 1);

    final EntityImpl document = new EntityImpl();
    document.field("prop8", 1);
    document.field("fEmbeddedSet", embeddedSet);

    assertEquals(containsDocument(result, document), 1);
  }

  @Test
  public void testCompositeSearchEqualsEmbeddedSetInMiddleIndex() {
    final List<Entity> result = tester.queryWithIndex(3, 3,
        "select * from sqlSelectIndexReuseTestClass where prop9 = 0 and fEmbeddedSetTwo contains 92 and prop8 > 2");

    final Set<Integer> embeddedSet = new HashSet<Integer>(3);
    embeddedSet.add(90);
    embeddedSet.add(91);
    embeddedSet.add(92);

    assertEquals(result.size(), 3);

    for (int i = 0; i < 3; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop8", i * 2 + 4);
      document.field("prop9", 0);
      document.field("fEmbeddedSet", embeddedSet);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchEqualsOneFieldEmbeddedListIndex() {
    final List<Entity> result = tester.queryWithIndex(2, 1,
        "select * from sqlSelectIndexReuseTestClass where fEmbeddedListTwo contains 4");

    assertEquals(result.size(), 10);

    final List<Integer> embeddedList = new ArrayList<Integer>(3);
    embeddedList.add(3);
    embeddedList.add(4);
    embeddedList.add(5);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop8", i);
      document.field("fEmbeddedListTwo", embeddedList);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchEqualsEmbeddedListIndex() {
    final List<Entity> result = tester.queryWithIndex(2, 2,
        "select * from sqlSelectIndexReuseTestClass where prop8 = 1 and fEmbeddedListTwo contains 4");

    assertEquals(result.size(), 1);

    final List<Integer> embeddedList = new ArrayList<Integer>(3);
    embeddedList.add(3);
    embeddedList.add(4);
    embeddedList.add(5);

    final EntityImpl document = new EntityImpl();
    document.field("prop8", 1);
    document.field("fEmbeddedListTwo", embeddedList);

    assertEquals(containsDocument(result, document), 1);
  }

  @Test
  public void testNoCompositeSearchEquals() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 = 1");

    assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", i);
      document.field("prop2", 1);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchEqualsWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 = ?", 1, 2);

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop2").intValue(), 2);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = ?", 1);

    assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testNoCompositeSearchEqualsWithArgs() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 = ?", 1);

    assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", i);
      document.field("prop2", 1);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchGT() {

    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 > 2");

    assertEquals(result.size(), 7);

    for (int i = 3; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchGTOneField() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 > 7");

    assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchGTOneFieldNoSearch() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 > 7");

    assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchGTWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 > ?", 1, 2);

    assertEquals(result.size(), 7);

    for (int i = 3; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchGTOneFieldWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 > ?", 7);

    assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchGTOneFieldNoSearchWithArgs() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 > ?", 7);

    assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchGTQ() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 >= 2");

    assertEquals(result.size(), 8);

    for (int i = 2; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchGTQOneField() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 >= 7");

    assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchGTQOneFieldNoSearch() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 >= 7");

    assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchGTQWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 >= ?", 1, 2);

    assertEquals(result.size(), 8);

    for (int i = 2; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchGTQOneFieldWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 >= ?", 7);

    assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchGTQOneFieldNoSearchWithArgs() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 >= ?", 7);

    assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchLTQ() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 <= 2");

    assertEquals(result.size(), 3);

    for (int i = 0; i <= 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchLTQOneField() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 <= 7");

    assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchLTQOneFieldNoSearch() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 <= 7");

    assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchLTQWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 <= ?", 1, 2);

    assertEquals(result.size(), 3);

    for (int i = 0; i <= 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchLTQOneFieldWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 <= ?", 7);

    assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchLTQOneFieldNoSearchWithArgs() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 <= ?", 7);

    assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchLT() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 < 2");

    assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchLTOneField() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 < 7");

    assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchLTOneFieldNoSearch() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 < 7");

    assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchLTWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 < ?", 1, 2);

    assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchLTOneFieldWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 < ?", 7);

    assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchLTOneFieldNoSearchWithArgs() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 < ?", 7);

    assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchBetween() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 between 1 and 3");

    assertEquals(result.size(), 3);

    for (int i = 1; i <= 3; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchBetweenOneField() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 between 1 and 3");

    assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchBetweenOneFieldNoSearch() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 between 1 and 3");

    assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchBetweenWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 between ? and ?",
        1, 3);

    assertEquals(result.size(), 3);

    for (int i = 1; i <= 3; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchBetweenOneFieldWithArgs() {
    final List<Entity> result = tester.queryWithIndex(2,
        "select * from sqlSelectIndexReuseTestClass where prop1 between ? and ?", 1, 3);

    assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", i);
        document.field("prop2", j);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testCompositeSearchBetweenOneFieldNoSearchWithArgs() {
    final List<Entity> result = tester.queryWithoutIndex(
        "select * from sqlSelectIndexReuseTestClass where prop2 between ? and ?", 1, 3);

    assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final EntityImpl document = new EntityImpl();
        document.field("prop1", j);
        document.field("prop2", i);

        assertEquals(containsDocument(result, document), 1);
      }
    }
  }

  @Test
  public void testSingleSearchEquals() {
    final List<Entity> result = tester.queryWithIndex(1,
        "select * from sqlSelectIndexReuseTestClass where prop3 = 1");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop3").intValue(), 1);
  }

  @Test
  public void testSingleSearchEqualsWithArgs() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 = ?", 1);

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop3").intValue(), 1);
  }

  @Test
  public void testSingleSearchGT() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 > 90");

    assertEquals(result.size(), 9);

    for (int i = 91; i < 100; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchGTWithArgs() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 > ?", 90);

    assertEquals(result.size(), 9);

    for (int i = 91; i < 100; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchGTQ() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 >= 90");

    assertEquals(result.size(), 10);

    for (int i = 90; i < 100; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchGTQWithArgs() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 >= ?", 90);

    assertEquals(result.size(), 10);

    for (int i = 90; i < 100; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchLTQ() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 <= 10");

    assertEquals(result.size(), 11);

    for (int i = 0; i <= 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchLTQWithArgs() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 <= ?", 10);

    assertEquals(result.size(), 11);

    for (int i = 0; i <= 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchLT() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 < 10");

    assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchLTWithArgs() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 < ?", 10);

    assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchBetween() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 between 1 and 10");

    assertEquals(result.size(), 10);

    for (int i = 1; i <= 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchBetweenWithArgs() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 between ? and ?", 1, 10);

    assertEquals(result.size(), 10);

    for (int i = 1; i <= 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchIN() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 in [0, 5, 10]");

    assertEquals(result.size(), 3);

    for (int i = 0; i <= 10; i += 5) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testSingleSearchINWithArgs() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where prop3 in [?, ?, ?]", 0, 5, 10);

    assertEquals(result.size(), 3);

    for (int i = 0; i <= 10; i += 5) {
      final EntityImpl document = new EntityImpl();
      document.field("prop3", i);
      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testMostSpecificOnesProcessedFirst() {
    final List<Entity> result =
        tester.queryWithIndex(2,
            "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 1 and"
                + " prop3 = 11");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop2").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop3").intValue(), 11);
  }

  @Test
  public void testTripleSearch() {
    final List<Entity> result =
        tester.queryWithIndex(3,
            "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 1 and"
                + " prop4 >= 1");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop2").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop4").intValue(), 1);
  }

  @Test
  public void testTripleSearchLastFieldNotInIndexFirstCase() {
    final List<Entity> result =
        tester.queryWithIndex(2,
            "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 1 and"
                + " prop5 >= 1");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop2").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop5").intValue(), 1);
  }

  @Test
  public void testTripleSearchLastFieldNotInIndexSecondCase() {
    final List<Entity> result =
        tester.queryWithIndex(2,
            "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop4 >= 1");

    assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);
      document.field("prop4", 1);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testTripleSearchLastFieldInIndex() {
    final List<Entity> result =
        tester.queryWithIndex(3,
            "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop4 = 1");

    assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop1", 1);
      document.field("prop2", i);
      document.field("prop4", 1);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testTripleSearchLastFieldsCanNotBeMerged() {
    final List<Entity> result =
        tester.queryWithIndex(3,
            "select * from sqlSelectIndexReuseTestClass where prop6 <= 1 and prop4 < 1");

    assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final EntityImpl document = new EntityImpl();
      document.field("prop6", i);
      document.field("prop4", 0);

      assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testLastFieldNotCompatibleOperator() {
    final List<Entity> result =
        tester.queryWithIndex(2,
            "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 + 1 = 3");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop2").intValue(), 2);
  }

  @Test
  public void testEmbeddedMapByKeyIndexReuse() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where fEmbeddedMap containskey"
                + " 'key12'");

    assertEquals(result.size(), 10);

    final EntityImpl document = new EntityImpl();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    document.field("fEmbeddedMap", embeddedMap);

    assertEquals(containsDocument(result, document), 10);
  }

  @Test
  public void testEmbeddedMapBySpecificKeyIndexReuse() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where ( fEmbeddedMap containskey"
                + " 'key12' ) and ( fEmbeddedMap['key12'] = 12 )");

    assertEquals(result.size(), 10);

    final EntityImpl document = new EntityImpl();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    document.field("fEmbeddedMap", embeddedMap);

    assertEquals(containsDocument(result, document), 10);
  }

  @Test
  public void testEmbeddedMapByValueIndexReuse() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where fEmbeddedMap containsvalue"
                + " 11");

    assertEquals(result.size(), 10);

    final EntityImpl document = new EntityImpl();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    document.field("fEmbeddedMap", embeddedMap);

    assertEquals(containsDocument(result, document), 10);
  }

  @Test
  public void testEmbeddedListIndexReuse() {
    final List<Entity> result =
        tester.queryWithIndex(1,
            "select * from sqlSelectIndexReuseTestClass where fEmbeddedList contains 7");

    final List<Integer> embeddedList = new ArrayList<Integer>(3);
    embeddedList.add(6);
    embeddedList.add(7);
    embeddedList.add(8);

    final EntityImpl document = new EntityImpl();
    document.field("fEmbeddedList", embeddedList);

    assertEquals(containsDocument(result, document), 10);
  }

  @Test
  public void testNotIndexOperatorFirstCase() {
    final List<Entity> result =
        tester.queryWithIndex(2,
            "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2  = 2 and"
                + " ( prop4 = 3 or prop4 = 1 )");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop2").intValue(), 2);
    assertEquals(document.<Integer>getProperty("prop4").intValue(), 1);
  }

  @Test
  public void testIndexUsedOnOrClause() {
    final List<Entity> result =
        tester.queryWithMultipleIndexes(2,
            "select * from sqlSelectIndexReuseTestClass where ( prop1 = 1 and prop2 = 2 )"
                + " or ( prop4  = 1 and prop6 = 2 )");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop2").intValue(), 2);
    assertEquals(document.<Integer>getProperty("prop4").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop6").intValue(), 2);

  }

  @Test
  public void testCompositeIndexEmptyResult() {
    final List<Entity> result =
        tester.queryWithIndex(2,
            "select * from sqlSelectIndexReuseTestClass where prop1 = 1777 and prop2  ="
                + " 2777");

    assertEquals(result.size(), 0);
  }

  @Test
  public void testReuseOfIndexOnSeveralClassesFields() {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass superClass = schema.createClass("sqlSelectIndexReuseTestSuperClass");
    superClass.createProperty(database, "prop0", PropertyType.INTEGER);
    final SchemaClass oClass = schema.createClass("sqlSelectIndexReuseTestChildClass", superClass);
    oClass.createProperty(database, "prop1", PropertyType.INTEGER);

    oClass.createIndex(database,
        "sqlSelectIndexReuseTestOnPropertiesFromClassAndSuperclass",
        SchemaClass.INDEX_TYPE.UNIQUE,
        "prop0", "prop1");

    database.begin();
    final EntityImpl docOne = new EntityImpl("sqlSelectIndexReuseTestChildClass");
    docOne.field("prop0", 0);
    docOne.field("prop1", 1);
    docOne.save();
    database.commit();

    database.begin();
    final EntityImpl docTwo = new EntityImpl("sqlSelectIndexReuseTestChildClass");
    docTwo.field("prop0", 2);
    docTwo.field("prop1", 3);
    docTwo.save();
    database.commit();

    final List<Entity> result =
        tester.queryWithIndex(2,
            "select * from sqlSelectIndexReuseTestChildClass where prop0 = 0 and prop1 = 1");

    assertEquals(result.size(), 1);
  }

  @Test
  public void testCountFunctionWithNotUniqueIndex() {
    SchemaClass klazz =
        database.getMetadata().getSchema().getOrCreateClass("CountFunctionWithNotUniqueIndexTest");
    if (!klazz.existsProperty("a")) {
      klazz.createProperty(database, "a", PropertyType.STRING);
      klazz.createIndex(database, "a", "NOTUNIQUE", "a");
    }

    database.begin();
    database
        .<EntityImpl>newInstance("CountFunctionWithNotUniqueIndexTest")
        .field("a", "a")
        .field("b", "b")
        .save();
    database
        .<EntityImpl>newInstance("CountFunctionWithNotUniqueIndexTest")
        .field("a", "a")
        .field("b", "b")
        .save();
    database
        .<EntityImpl>newInstance("CountFunctionWithNotUniqueIndexTest")
        .field("a", "a")
        .field("b", "e")
        .save();
    database
        .<EntityImpl>newInstance("CountFunctionWithNotUniqueIndexTest")
        .field("a", "c")
        .field("b", "c")
        .save();
    database.commit();

    try (var rs = database.query(
        "select count(*) as count from CountFunctionWithNotUniqueIndexTest where a = 'a' and"
            + " b = 'c'")) {
      if (!remoteDB) {
        assertEquals(indexesUsed(rs.getExecutionPlan().orElseThrow()), 1);
      }
      assertEquals(rs.findFirst().<Long>getProperty("count"), 0L);
    }
  }

  @Test
  public void testCountFunctionWithUniqueIndex() {
    SchemaClass klazz =
        database.getMetadata().getSchema().getOrCreateClass("CountFunctionWithUniqueIndexTest");
    if (!klazz.existsProperty("a")) {
      klazz.createProperty(database, "a", PropertyType.STRING);
      klazz.createIndex(database, "testCountFunctionWithUniqueIndex", "NOTUNIQUE", "a");
    }

    database.begin();
    database
        .<EntityImpl>newInstance("CountFunctionWithUniqueIndexTest")
        .field("a", "a")
        .field("b", "c")
        .save();
    database
        .<EntityImpl>newInstance("CountFunctionWithUniqueIndexTest")
        .field("a", "a")
        .field("b", "c")
        .save();
    database
        .<EntityImpl>newInstance("CountFunctionWithUniqueIndexTest")
        .field("a", "a")
        .field("b", "e")
        .save();
    Entity doc =
        database
            .<EntityImpl>newInstance("CountFunctionWithUniqueIndexTest")
            .field("a", "a")
            .field("b", "b");
    doc.save();
    database.commit();

    try (var rs = database.query(
        "select count(*) as count from CountFunctionWithUniqueIndexTest where a = 'a' and b"
            + " = 'c'")) {
      if (!remoteDB) {
        assertEquals(indexesUsed(rs.getExecutionPlan().orElseThrow()), 1);
      }
      assertEquals(rs.findFirst().<Long>getProperty("count"), 2L);
    }

    database.begin();
    database.bindToSession(doc).delete();
    database.commit();
  }

  private static int containsDocument(final List<Entity> docList,
      final EntityImpl document) {
    int count = 0;
    for (final Entity docItem : docList) {
      boolean containsAllFields = true;
      for (final String fieldName : document.fieldNames()) {
        if (!document.field(fieldName).equals(docItem.getProperty(fieldName))) {
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
  public void testCompositeSearchIn1() {
    final List<Entity> result =
        tester.queryWithIndex(3, 3,
            "select * from sqlSelectIndexReuseTestClass where prop4 = 1 and prop1 = 1 and"
                + " prop3 in [13, 113]");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop4").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop3").intValue(), 13);
  }

  @Test
  public void testCompositeSearchIn2() {
    final List<Entity> result =
        tester.queryWithIndex(3, 1,
            "select * from sqlSelectIndexReuseTestClass where prop4 = 1 and prop1 in [1, 2]"
                + " and prop3 = 13");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop4").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop3").intValue(), 13);
  }

  @Test
  public void testCompositeSearchIn3() {
    final List<Entity> result =
        tester.queryWithIndex(3, 1,
            "select * from sqlSelectIndexReuseTestClass where prop4 = 1 and prop1 in [1, 2]"
                + " and prop3 in [13, 15]");

    assertEquals(result.size(), 2);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop4").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    Assert.assertTrue(
        document.<Integer>getProperty("prop3").equals(13) || document.<Integer>getProperty("prop3").equals(15));
  }

  @Test
  public void testCompositeSearchIn4() {
    final List<Entity> result =
        tester.queryWithIndex(2, 1,
            "select * from sqlSelectIndexReuseTestClass where prop4 in [1, 2] and prop1 = 1"
                + " and prop3 = 13");

    assertEquals(result.size(), 1);

    final Entity document = result.get(0);
    assertEquals(document.<Integer>getProperty("prop4").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop1").intValue(), 1);
    assertEquals(document.<Integer>getProperty("prop3").intValue(), 13);
  }
}
