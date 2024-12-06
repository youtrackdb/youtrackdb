package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TruncateClassStatementExecutionTest extends BaseMemoryInternalDatabase {

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClass() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass testClass = getOrCreateClass(schema);

    final Index index = getOrCreateIndex(testClass);

    db.command("truncate class test_class");

    db.begin();
    db.save(new EntityImpl(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    db.save(new EntityImpl(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));
    db.commit();

    db.command("truncate class test_class").close();

    db.begin();
    db.save(new EntityImpl(testClass).field("name", "x").field("data", Arrays.asList(5, 6, 7)));
    db.save(new EntityImpl(testClass).field("name", "y").field("data", Arrays.asList(8, 9, -1)));
    db.commit();

    ResultSet result = db.query("select from test_class");
    //    Assert.assertEquals(result.size(), 2);

    Set<Integer> set = new HashSet<Integer>();
    while (result.hasNext()) {
      set.addAll(result.next().getProperty("data"));
    }
    result.close();
    Assert.assertTrue(set.containsAll(Arrays.asList(5, 6, 7, 8, 9, -1)));

    Assert.assertEquals(index.getInternal().size(db), 6);

    try (Stream<RawPair<Object, RID>> stream = index.getInternal().stream(db)) {
      stream.forEach(
          (entry) -> {
            Assert.assertTrue(set.contains((Integer) entry.first));
          });
    }

    schema.dropClass("test_class");
  }

  @Test
  public void testTruncateVertexClass() {
    db.command("create class TestTruncateVertexClass extends V");

    db.begin();
    db.command("create vertex TestTruncateVertexClass set name = 'foo'");
    db.commit();

    try {
      db.command("truncate class TestTruncateVertexClass");
      Assert.fail();
    } catch (Exception e) {
    }
    ResultSet result = db.query("select from TestTruncateVertexClass");
    Assert.assertTrue(result.hasNext());
    result.close();

    db.command("truncate class TestTruncateVertexClass unsafe");
    result = db.query("select from TestTruncateVertexClass");
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTruncateVertexClassSubclasses() {

    db.command("create class TestTruncateVertexClassSuperclass");
    db.command(
        "create class TestTruncateVertexClassSubclass extends TestTruncateVertexClassSuperclass");

    db.begin();
    db.command("insert into TestTruncateVertexClassSuperclass set name = 'foo'");
    db.command("insert into TestTruncateVertexClassSubclass set name = 'bar'");
    db.commit();

    ResultSet result = db.query("select from TestTruncateVertexClassSuperclass");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();

    db.command("truncate class TestTruncateVertexClassSuperclass ");
    result = db.query("select from TestTruncateVertexClassSubclass");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();

    db.command("truncate class TestTruncateVertexClassSuperclass polymorphic");
    result = db.query("select from TestTruncateVertexClassSubclass");
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTruncateVertexClassSubclassesWithIndex() {

    db.command("create class TestTruncateVertexClassSuperclassWithIndex");
    db.command("create property TestTruncateVertexClassSuperclassWithIndex.name STRING");
    db.command(
        "create index TestTruncateVertexClassSuperclassWithIndex_index on"
            + " TestTruncateVertexClassSuperclassWithIndex (name) NOTUNIQUE");

    db.command(
        "create class TestTruncateVertexClassSubclassWithIndex extends"
            + " TestTruncateVertexClassSuperclassWithIndex");

    db.begin();
    db.command("insert into TestTruncateVertexClassSuperclassWithIndex set name = 'foo'");
    db.command("insert into TestTruncateVertexClassSubclassWithIndex set name = 'bar'");
    db.commit();

    if (!db.getStorage().isRemote()) {
      final IndexManagerAbstract indexManager = db.getMetadata().getIndexManagerInternal();
      final Index indexOne =
          indexManager.getIndex(db, "TestTruncateVertexClassSuperclassWithIndex_index");
      Assert.assertEquals(2, indexOne.getInternal().size(db));

      db.command("truncate class TestTruncateVertexClassSubclassWithIndex");
      Assert.assertEquals(1, indexOne.getInternal().size(db));

      db.command("truncate class TestTruncateVertexClassSuperclassWithIndex polymorphic");
      Assert.assertEquals(0, indexOne.getInternal().size(db));
    }
  }

  private List<Result> toList(ResultSet input) {
    List<Result> result = new ArrayList<>();
    while (input.hasNext()) {
      result.add(input.next());
    }
    return result;
  }

  private Index getOrCreateIndex(SchemaClass testClass) {
    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "test_class_by_data");
    if (index == null) {
      testClass.createProperty(db, "data", PropertyType.EMBEDDEDLIST, PropertyType.INTEGER);
      index = testClass.createIndex(db, "test_class_by_data", SchemaClass.INDEX_TYPE.UNIQUE,
          "data");
    }
    return index;
  }

  private SchemaClass getOrCreateClass(Schema schema) {
    SchemaClass testClass;
    if (schema.existsClass("test_class")) {
      testClass = schema.getClass("test_class");
    } else {
      testClass = schema.createClass("test_class");
    }
    return testClass;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClassWithCommandCache() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass testClass = getOrCreateClass(schema);

    db.command("truncate class test_class");

    db.begin();
    db.save(new EntityImpl(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    db.save(new EntityImpl(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));
    db.commit();

    ResultSet result = db.query("select from test_class");
    Assert.assertEquals(toList(result).size(), 2);

    result.close();
    db.command("truncate class test_class");

    result = db.query("select from test_class");
    Assert.assertEquals(toList(result).size(), 0);
    result.close();

    schema.dropClass("test_class");
  }
}
