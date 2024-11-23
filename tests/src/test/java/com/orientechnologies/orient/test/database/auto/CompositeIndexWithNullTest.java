package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Parameters;

/**
 * @since 4/11/14
 */
public class CompositeIndexWithNullTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public CompositeIndexWithNullTest(boolean remote) {
    super(remote);
  }

  public void testPointQuery() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryClass");
    clazz.createProperty(database, "prop1", OType.INTEGER);
    clazz.createProperty(database, "prop2", OType.INTEGER);
    clazz.createProperty(database, "prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullPointQueryIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullPointQueryClass");
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
    List<OElement> result =
        database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      OElement document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryIndex"));

    query =
        "select from compositeIndexNullPointQueryClass where prop1 = 1 and prop2 = 2 and prop3 is"
            + " null";
    result = database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (OElement document : result) {
      Assert.assertNull(document.getProperty("prop3"));
    }

    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryIndex"));
  }

  public void testPointQueryInTx() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryInTxClass");
    clazz.createProperty(database, "prop1", OType.INTEGER);
    clazz.createProperty(database, "prop2", OType.INTEGER);
    clazz.createProperty(database, "prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullPointQueryInTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullPointQueryInTxClass");
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
    List<OElement> result =
        database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      OElement document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInTxIndex"));

    query =
        "select from compositeIndexNullPointQueryInTxClass where prop1 = 1 and prop2 = 2 and prop3"
            + " is null";
    result = database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (OElement document : result) {
      Assert.assertNull(document.getProperty("prop3"));
    }

    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInTxIndex"));
  }

  public void testPointQueryInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryInMiddleTxClass");
    clazz.createProperty(database, "prop1", OType.INTEGER);
    clazz.createProperty(database, "prop2", OType.INTEGER);
    clazz.createProperty(database, "prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullPointQueryInMiddleTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullPointQueryInMiddleTxClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      document.save();
    }

    String query =
        "select from compositeIndexNullPointQueryInMiddleTxClass where prop1 = 1 and prop2 = 2";
    List<OElement> result =
        database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 5);

    for (int k = 0; k < 5; k++) {
      OElement document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInMiddleTxIndex"));

    query =
        "select from compositeIndexNullPointQueryInMiddleTxClass where prop1 = 1 and prop2 = 2 and"
            + " prop3 is null";
    result = database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (OElement document : result) {
      Assert.assertNull(document.getProperty("prop3"));
    }

    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryInMiddleTxIndex"));

    database.commit();
  }

  public void testRangeQuery() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullRangeQueryClass");
    clazz.createProperty(database, "prop1", OType.INTEGER);
    clazz.createProperty(database, "prop2", OType.INTEGER);
    clazz.createProperty(database, "prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullRangeQueryIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullRangeQueryClass");
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
    List<OElement> result =
        database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      OElement document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertTrue(document.<Integer>getProperty("prop2") > 2);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryIndex"));

    query = "select from compositeIndexNullRangeQueryClass where prop1 > 0";
    result = database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      OElement document = result.get(k);
      Assert.assertTrue(document.<Integer>getProperty("prop1") > 0);
    }
  }

  public void testRangeQueryInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullRangeQueryInMiddleTxClass");
    clazz.createProperty(database, "prop1", OType.INTEGER);
    clazz.createProperty(database, "prop2", OType.INTEGER);
    clazz.createProperty(database, "prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullRangeQueryInMiddleTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    database.begin();
    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullRangeQueryInMiddleTxClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      document.save();
    }

    String query =
        "select from compositeIndexNullRangeQueryInMiddleTxClass where prop1 = 1 and prop2 > 2";
    List<OElement> result =
        database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      OElement document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertTrue(document.<Integer>getProperty("prop2") > 2);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryInMiddleTxIndex"));

    query = "select from compositeIndexNullRangeQueryInMiddleTxClass where prop1 > 0";
    result = database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      OElement document = result.get(k);
      Assert.assertTrue(document.<Integer>getProperty("prop1") > 0);
    }

    database.commit();
  }

  public void testPointQueryNullInTheMiddle() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryNullInTheMiddleClass");
    clazz.createProperty(database, "prop1", OType.INTEGER);
    clazz.createProperty(database, "prop2", OType.INTEGER);
    clazz.createProperty(database, "prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullPointQueryNullInTheMiddleIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullPointQueryNullInTheMiddleClass");
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
    List<OElement> result =
        database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      OElement document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleClass where prop1 = 1 and prop2 is"
            + " null";
    result = database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (OElement document : result) {
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertNull(document.getProperty("prop2"));
    }

    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleClass where prop1 = 1 and prop2 is"
            + " null and prop3 = 13";
    result = database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);

    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));
  }

  public void testPointQueryNullInTheMiddleInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass");
    clazz.createProperty(database, "prop1", OType.INTEGER);
    clazz.createProperty(database, "prop2", OType.INTEGER);
    clazz.createProperty(database, "prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null, new String[]{"prop1", "prop2", "prop3"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      ODocument document =
          new ODocument("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass");
      document.field("prop1", i / 10);

      if (i % 2 == 0) {
        document.field("prop2", i);
      }

      document.field("prop3", i);

      document.save();
    }

    String query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1";
    List<OElement> result =
        database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      OElement document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1 and"
            + " prop2 is null";
    result = database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 5);
    for (OElement document : result) {
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      Assert.assertNull(document.getProperty("prop2"));
    }

    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex"));

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1 and"
            + " prop2 is null and prop3 = 13";
    result = database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);

    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex"));

    database.commit();
  }

  public void testRangeQueryNullInTheMiddle() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullRangeQueryNullInTheMiddleClass");
    clazz.createProperty(database, "prop1", OType.INTEGER);
    clazz.createProperty(database, "prop2", OType.INTEGER);
    clazz.createProperty(database, "prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullRangeQueryNullInTheMiddleIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullRangeQueryNullInTheMiddleClass");
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
    List<OElement> result =
        database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      OElement document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryNullInTheMiddleIndex"));
  }

  public void testRangeQueryNullInTheMiddleInMiddleTx() {
    if (remoteDB) {
      return;
    }

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass");
    clazz.createProperty(database, "prop1", OType.INTEGER);
    clazz.createProperty(database, "prop2", OType.INTEGER);
    clazz.createProperty(database, "prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(database,
        "compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata, new String[]{"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      ODocument document =
          new ODocument("compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass");
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
    List<OElement> result =
        database.query(query).stream().map((r) -> r.toElement()).collect(Collectors.toList());
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      OElement document = result.get(k);
      Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxIndex"));
  }
}
