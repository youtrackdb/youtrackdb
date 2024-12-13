package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @since 4/11/14
 */
public class CompositeIndexWithNullTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public CompositeIndexWithNullTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testPointQuery() {
    final Schema schema = database.getMetadata().getSchema();
    SchemaClassInternal clazz = (SchemaClassInternal) schema.createClass(
        "compositeIndexNullPointQueryClass");
    clazz.createProperty(database, "prop1", PropertyType.INTEGER);
    clazz.createProperty(database, "prop2", PropertyType.INTEGER);
    clazz.createProperty(database, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullPointQueryIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document = new EntityImpl("compositeIndexNullPointQueryClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      database.begin();
      document.save();
      database.commit();
    }

    String query = "select from compositeIndexNullPointQueryClass where prop1 = 1 and prop2 = 2";
    List<Entity> result =
        database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
    }

    EntityImpl explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryIndex"));

    query =
        "select from compositeIndexNullPointQueryClass where prop1 = 1 and prop2 = 2 and prop3 is"
            + " null";
    result = database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (Entity document : result) {
      Assert.assertNull(document.getProperty("prop3"));
    }

    explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryIndex"));
  }

  public void testPointQueryInTx() {
    final Schema schema = database.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullPointQueryInTxClass");
    clazz.createProperty(database, "prop1", PropertyType.INTEGER);
    clazz.createProperty(database, "prop2", PropertyType.INTEGER);
    clazz.createProperty(database, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);
    clazz.createIndex(database,
        "compositeIndexNullPointQueryInTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      EntityImpl document = new EntityImpl("compositeIndexNullPointQueryInTxClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      document.save();
    }

    database.commit();

    String query =
        "select from compositeIndexNullPointQueryInTxClass where prop1 = 1 and prop2 = 2";
    List<Entity> result =
        database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
    }

    EntityImpl explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInTxIndex"));

    query =
        "select from compositeIndexNullPointQueryInTxClass where prop1 = 1 and prop2 = 2 and prop3"
            + " is null";
    result = database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (Entity document : result) {
      Assert.assertNull(document.getProperty("prop3"));
    }

    explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInTxIndex"));
  }

  public void testPointQueryInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final Schema schema = database.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullPointQueryInMiddleTxClass");
    clazz.createProperty(database, "prop1", PropertyType.INTEGER);
    clazz.createProperty(database, "prop2", PropertyType.INTEGER);
    clazz.createProperty(database, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullPointQueryInMiddleTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      EntityImpl document = new EntityImpl("compositeIndexNullPointQueryInMiddleTxClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      document.save();
    }

    String query =
        "select from compositeIndexNullPointQueryInMiddleTxClass where prop1 = 1 and prop2 = 2";
    List<Entity> result =
        database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 5);

    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
    }

    EntityImpl explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInMiddleTxIndex"));

    query =
        "select from compositeIndexNullPointQueryInMiddleTxClass where prop1 = 1 and prop2 = 2 and"
            + " prop3 is null";
    result = database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (Entity document : result) {
      Assert.assertNull(document.getProperty("prop3"));
    }

    explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInMiddleTxIndex"));

    database.commit();
  }

  public void testRangeQuery() {
    final Schema schema = database.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullRangeQueryClass");
    clazz.createProperty(database, "prop1", PropertyType.INTEGER);
    clazz.createProperty(database, "prop2", PropertyType.INTEGER);
    clazz.createProperty(database, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullRangeQueryIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document = new EntityImpl("compositeIndexNullRangeQueryClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      database.begin();
      document.save();
      database.commit();
    }

    String query = "select from compositeIndexNullRangeQueryClass where prop1 = 1 and prop2 > 2";
    List<Entity> result =
        database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertTrue(document.<Integer>getProperty("prop2") > 2);
    }

    EntityImpl explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryIndex"));

    query = "select from compositeIndexNullRangeQueryClass where prop1 > 0";
    result = database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertTrue(document.<Integer>getProperty("prop1") > 0);
    }
  }

  public void testRangeQueryInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final Schema schema = database.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullRangeQueryInMiddleTxClass");
    clazz.createProperty(database, "prop1", PropertyType.INTEGER);
    clazz.createProperty(database, "prop2", PropertyType.INTEGER);
    clazz.createProperty(database, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullRangeQueryInMiddleTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    database.begin();
    for (int i = 0; i < 20; i++) {
      EntityImpl document = new EntityImpl("compositeIndexNullRangeQueryInMiddleTxClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      document.save();
    }

    String query =
        "select from compositeIndexNullRangeQueryInMiddleTxClass where prop1 = 1 and prop2 > 2";
    List<Entity> result =
        database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertTrue(document.<Integer>getProperty("prop2") > 2);
    }

    EntityImpl explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryInMiddleTxIndex"));

    query = "select from compositeIndexNullRangeQueryInMiddleTxClass where prop1 > 0";
    result = database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertTrue(document.<Integer>getProperty("prop1") > 0);
    }

    database.commit();
  }

  public void testPointQueryNullInTheMiddle() {
    final Schema schema = database.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullPointQueryNullInTheMiddleClass");
    clazz.createProperty(database, "prop1", PropertyType.INTEGER);
    clazz.createProperty(database, "prop2", PropertyType.INTEGER);
    clazz.createProperty(database, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullPointQueryNullInTheMiddleIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document = new EntityImpl("compositeIndexNullPointQueryNullInTheMiddleClass");
      document.field("prop1", i / 10);

      if (i % 2 == 0) {
        document.field("prop2", i);
      }

      document.field("prop3", i);

      database.begin();
      document.save();
      database.commit();
    }

    String query = "select from compositeIndexNullPointQueryNullInTheMiddleClass where prop1 = 1";
    List<Entity> result =
        database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    EntityImpl explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleClass where prop1 = 1 and prop2 is"
            + " null";
    result = database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (Entity document : result) {
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertNull(document.getProperty("prop2"));
    }

    explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleClass where prop1 = 1 and prop2 is"
            + " null and prop3 = 13";
    result = database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);

    explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));
  }

  public void testPointQueryNullInTheMiddleInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final Schema schema = database.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass(
        "compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass");
    clazz.createProperty(database, "prop1", PropertyType.INTEGER);
    clazz.createProperty(database, "prop2", PropertyType.INTEGER);
    clazz.createProperty(database, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);
    clazz.createIndex(database,
        "compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      EntityImpl document =
          new EntityImpl("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass");
      document.field("prop1", i / 10);

      if (i % 2 == 0) {
        document.field("prop2", i);
      }

      document.field("prop3", i);

      document.save();
    }

    String query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1";
    List<Entity> result =
        database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    EntityImpl explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1 and"
            + " prop2 is null";
    result = database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (Entity document : result) {
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertNull(document.getProperty("prop2"));
    }

    explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1 and"
            + " prop2 is null and prop3 = 13";
    result = database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);

    explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex"));

    database.commit();
  }

  public void testRangeQueryNullInTheMiddle() {
    final Schema schema = database.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullRangeQueryNullInTheMiddleClass");
    clazz.createProperty(database, "prop1", PropertyType.INTEGER);
    clazz.createProperty(database, "prop2", PropertyType.INTEGER);
    clazz.createProperty(database, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullRangeQueryNullInTheMiddleIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document = new EntityImpl("compositeIndexNullRangeQueryNullInTheMiddleClass");
      document.field("prop1", i / 10);

      if (i % 2 == 0) {
        document.field("prop2", i);
      }

      document.field("prop3", i);

      database.begin();
      document.save();
      database.commit();
    }

    final String query =
        "select from compositeIndexNullRangeQueryNullInTheMiddleClass where prop1 > 0";
    List<Entity> result =
        database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    EntityImpl explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryNullInTheMiddleIndex"));
  }

  public void testRangeQueryNullInTheMiddleInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final Schema schema = database.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass(
        "compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass");
    clazz.createProperty(database, "prop1", PropertyType.INTEGER);
    clazz.createProperty(database, "prop2", PropertyType.INTEGER);
    clazz.createProperty(database, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);
    clazz.createIndex(database,
        "compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document =
          new EntityImpl("compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass");
      document.field("prop1", i / 10);

      if (i % 2 == 0) {
        document.field("prop2", i);
      }

      document.field("prop3", i);

      database.begin();
      document.save();
      database.commit();
    }

    final String query =
        "select from compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass where prop1 > 0";
    List<Entity> result =
        database.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    EntityImpl explain = database.command(new CommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxIndex"));
  }
}
