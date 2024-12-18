package com.jetbrains.youtrack.db.auto;

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
public class CompositeIndexWithNullTest extends BaseDBTest {

  @Parameters(value = "remote")
  public CompositeIndexWithNullTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testPointQuery() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClassInternal clazz = (SchemaClassInternal) schema.createClass(
        "compositeIndexNullPointQueryClass");
    clazz.createProperty(db, "prop1", PropertyType.INTEGER);
    clazz.createProperty(db, "prop2", PropertyType.INTEGER);
    clazz.createProperty(db, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "compositeIndexNullPointQueryIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity("compositeIndexNullPointQueryClass"));
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      db.begin();
      document.save();
      db.commit();
    }

    String query = "select from compositeIndexNullPointQueryClass where prop1 = 1 and prop2 = 2";
    List<Entity> result =
        db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
    }

    EntityImpl explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryIndex"));

    query =
        "select from compositeIndexNullPointQueryClass where prop1 = 1 and prop2 = 2 and prop3 is"
            + " null";
    result = db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (Entity document : result) {
      Assert.assertNull(document.getProperty("prop3"));
    }

    explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryIndex"));
  }

  public void testPointQueryInTx() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullPointQueryInTxClass");
    clazz.createProperty(db, "prop1", PropertyType.INTEGER);
    clazz.createProperty(db, "prop2", PropertyType.INTEGER);
    clazz.createProperty(db, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);
    clazz.createIndex(db,
        "compositeIndexNullPointQueryInTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    db.begin();

    for (int i = 0; i < 20; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity("compositeIndexNullPointQueryInTxClass"));
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      document.save();
    }

    db.commit();

    String query =
        "select from compositeIndexNullPointQueryInTxClass where prop1 = 1 and prop2 = 2";
    List<Entity> result =
        db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
    }

    EntityImpl explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInTxIndex"));

    query =
        "select from compositeIndexNullPointQueryInTxClass where prop1 = 1 and prop2 = 2 and prop3"
            + " is null";
    result = db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (Entity document : result) {
      Assert.assertNull(document.getProperty("prop3"));
    }

    explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInTxIndex"));
  }

  public void testPointQueryInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullPointQueryInMiddleTxClass");
    clazz.createProperty(db, "prop1", PropertyType.INTEGER);
    clazz.createProperty(db, "prop2", PropertyType.INTEGER);
    clazz.createProperty(db, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "compositeIndexNullPointQueryInMiddleTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    db.begin();

    for (int i = 0; i < 20; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity(
          "compositeIndexNullPointQueryInMiddleTxClass"));
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
        db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 5);

    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
    }

    EntityImpl explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInMiddleTxIndex"));

    query =
        "select from compositeIndexNullPointQueryInMiddleTxClass where prop1 = 1 and prop2 = 2 and"
            + " prop3 is null";
    result = db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (Entity document : result) {
      Assert.assertNull(document.getProperty("prop3"));
    }

    explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInMiddleTxIndex"));

    db.commit();
  }

  public void testRangeQuery() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullRangeQueryClass");
    clazz.createProperty(db, "prop1", PropertyType.INTEGER);
    clazz.createProperty(db, "prop2", PropertyType.INTEGER);
    clazz.createProperty(db, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "compositeIndexNullRangeQueryIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity("compositeIndexNullRangeQueryClass"));
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      db.begin();
      document.save();
      db.commit();
    }

    String query = "select from compositeIndexNullRangeQueryClass where prop1 = 1 and prop2 > 2";
    List<Entity> result =
        db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertTrue(document.<Integer>getProperty("prop2") > 2);
    }

    EntityImpl explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryIndex"));

    query = "select from compositeIndexNullRangeQueryClass where prop1 > 0";
    result = db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

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

    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullRangeQueryInMiddleTxClass");
    clazz.createProperty(db, "prop1", PropertyType.INTEGER);
    clazz.createProperty(db, "prop2", PropertyType.INTEGER);
    clazz.createProperty(db, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "compositeIndexNullRangeQueryInMiddleTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    db.begin();
    for (int i = 0; i < 20; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity(
          "compositeIndexNullRangeQueryInMiddleTxClass"));
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
        db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertTrue(document.<Integer>getProperty("prop2") > 2);
    }

    EntityImpl explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryInMiddleTxIndex"));

    query = "select from compositeIndexNullRangeQueryInMiddleTxClass where prop1 > 0";
    result = db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertTrue(document.<Integer>getProperty("prop1") > 0);
    }

    db.commit();
  }

  public void testPointQueryNullInTheMiddle() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullPointQueryNullInTheMiddleClass");
    clazz.createProperty(db, "prop1", PropertyType.INTEGER);
    clazz.createProperty(db, "prop2", PropertyType.INTEGER);
    clazz.createProperty(db, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "compositeIndexNullPointQueryNullInTheMiddleIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity(
          "compositeIndexNullPointQueryNullInTheMiddleClass"));
      document.field("prop1", i / 10);

      if (i % 2 == 0) {
        document.field("prop2", i);
      }

      document.field("prop3", i);

      db.begin();
      document.save();
      db.commit();
    }

    String query = "select from compositeIndexNullPointQueryNullInTheMiddleClass where prop1 = 1";
    List<Entity> result =
        db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    EntityImpl explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleClass where prop1 = 1 and prop2 is"
            + " null";
    result = db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (Entity document : result) {
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertNull(document.getProperty("prop2"));
    }

    explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleClass where prop1 = 1 and prop2 is"
            + " null and prop3 = 13";
    result = db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);

    explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));
  }

  public void testPointQueryNullInTheMiddleInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass(
        "compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass");
    clazz.createProperty(db, "prop1", PropertyType.INTEGER);
    clazz.createProperty(db, "prop2", PropertyType.INTEGER);
    clazz.createProperty(db, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);
    clazz.createIndex(db,
        "compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    db.begin();

    for (int i = 0; i < 20; i++) {
      EntityImpl document =
          ((EntityImpl) db.newEntity("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass"));
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
        db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    EntityImpl explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1 and"
            + " prop2 is null";
    result = db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (Entity document : result) {
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertNull(document.getProperty("prop2"));
    }

    explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1 and"
            + " prop2 is null and prop3 = 13";
    result = db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);

    explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex"));

    db.commit();
  }

  public void testRangeQueryNullInTheMiddle() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("compositeIndexNullRangeQueryNullInTheMiddleClass");
    clazz.createProperty(db, "prop1", PropertyType.INTEGER);
    clazz.createProperty(db, "prop2", PropertyType.INTEGER);
    clazz.createProperty(db, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);

    clazz.createIndex(db,
        "compositeIndexNullRangeQueryNullInTheMiddleIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity(
          "compositeIndexNullRangeQueryNullInTheMiddleClass"));
      document.field("prop1", i / 10);

      if (i % 2 == 0) {
        document.field("prop2", i);
      }

      document.field("prop3", i);

      db.begin();
      document.save();
      db.commit();
    }

    final String query =
        "select from compositeIndexNullRangeQueryNullInTheMiddleClass where prop1 > 0";
    List<Entity> result =
        db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    EntityImpl explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryNullInTheMiddleIndex"));
  }

  public void testRangeQueryNullInTheMiddleInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass(
        "compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass");
    clazz.createProperty(db, "prop1", PropertyType.INTEGER);
    clazz.createProperty(db, "prop2", PropertyType.INTEGER);
    clazz.createProperty(db, "prop3", PropertyType.INTEGER);

    var metadata = Map.of("ignoreNullValues", false);
    clazz.createIndex(db,
        "compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxIndex",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      EntityImpl document =
          ((EntityImpl) db.newEntity("compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass"));
      document.field("prop1", i / 10);

      if (i % 2 == 0) {
        document.field("prop2", i);
      }

      document.field("prop3", i);

      db.begin();
      document.save();
      db.commit();
    }

    final String query =
        "select from compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass where prop1 > 0";
    List<Entity> result =
        db.query(query).stream().map((r) -> r.toEntity()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      Entity document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    EntityImpl explain = db.command(new CommandSQL("explain " + query)).execute(db);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxIndex"));
  }
}
