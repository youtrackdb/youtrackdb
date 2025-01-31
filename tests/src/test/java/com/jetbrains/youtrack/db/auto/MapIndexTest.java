package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 21.12.11
 */
@Test
public class MapIndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public MapIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    if (db.getMetadata().getSchema().existsClass("Mapper")) {
      db.getMetadata().getSchema().dropClass("Mapper");
    }

    final var mapper = db.getMetadata().getSchema().createClass("Mapper");
    mapper.createProperty(db, "id", PropertyType.STRING);
    mapper.createProperty(db, "intMap", PropertyType.EMBEDDEDMAP, PropertyType.INTEGER);

    mapper.createIndex(db, "mapIndexTestKey", SchemaClass.INDEX_TYPE.NOTUNIQUE, "intMap");
    mapper.createIndex(db, "mapIndexTestValue", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "intMap by value");

    final var movie = db.getMetadata().getSchema().createClass("MapIndexTestMovie");
    movie.createProperty(db, "title", PropertyType.STRING);
    movie.createProperty(db, "thumbs", PropertyType.EMBEDDEDMAP, PropertyType.INTEGER);

    movie.createIndex(db, "indexForMap", SchemaClass.INDEX_TYPE.NOTUNIQUE, "thumbs by key");
  }

  @AfterClass
  public void destroySchema() {
    db = createSessionInstance();
    db.getMetadata().getSchema().dropClass("Mapper");
    db.getMetadata().getSchema().dropClass("MapIndexTestMovie");
    db.close();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    db.begin();
    db.command("delete from Mapper").close();
    db.command("delete from MapIndexTestMovie").close();
    db.commit();

    super.afterMethod();
  }

  public void testIndexMap() {
    checkEmbeddedDB();

    final var mapper = db.newEntity("Mapper");
    final Map<String, Integer> map = new HashMap<>();
    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    db.begin();
    db.save(mapper);
    db.commit();

    final var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 2);
    try (final var keyStream = keyIndex.getInternal().keyStream()) {
      final var keyIterator = keyStream.iterator();
      while (keyIterator.hasNext()) {
        final var key = (String) keyIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);
    try (final var valueStream = valueIndex.getInternal().keyStream()) {
      final var valuesIterator = valueStream.iterator();
      while (valuesIterator.hasNext()) {
        final var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapInTx() {
    checkEmbeddedDB();

    try {
      db.begin();
      final var mapper = db.newEntity("Mapper");
      Map<String, Integer> map = new HashMap<>();

      map.put("key1", 10);
      map.put("key2", 20);

      mapper.setProperty("intMap", map);
      db.save(mapper);
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(db), 2);
    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOne() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> mapOne = new HashMap<>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setProperty("intMap", mapOne);
    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();

    mapper = db.bindToSession(mapper);
    final Map<String, Integer> mapTwo = new HashMap<>();

    mapTwo.put("key3", 30);
    mapTwo.put("key2", 20);

    mapper.setProperty("intMap", mapTwo);

    db.save(mapper);
    db.commit();

    var keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(30) && !value.equals(20)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTx() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> mapOne = new HashMap<>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setProperty("intMap", mapOne);
    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();
    try {
      final Map<String, Integer> mapTwo = new HashMap<>();

      mapTwo.put("key3", 30);
      mapTwo.put("key2", 20);

      mapper = db.bindToSession(mapper);
      mapper.setProperty("intMap", mapTwo);
      db.save(mapper);
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(30) && !value.equals(20)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTxRollback() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> mapOne = new HashMap<>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setProperty("intMap", mapOne);
    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();
    final Map<String, Integer> mapTwo = new HashMap<>();

    mapTwo.put("key3", 30);
    mapTwo.put("key2", 20);

    mapper = db.bindToSession(mapper);
    mapper.setProperty("intMap", mapTwo);
    db.save(mapper);
    db.rollback();

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key1")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItem() {
    checkEmbeddedDB();

    db.begin();
    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    mapper = db.save(mapper);
    db.commit();

    db.begin();
    db.command("UPDATE " + mapper.getIdentity() + " set intMap['key3'] = 30").close();
    db.commit();

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 3);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 3);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(30) && !value.equals(20) && !value.equals(10)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItemTx() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    db.begin();
    mapper = db.save(mapper);
    db.commit();

    try {
      db.begin();
      Entity loadedMapper = db.load(mapper.getIdentity());
      loadedMapper.<Map<String, Integer>>getProperty("intMap").put("key3", 30);
      db.save(loadedMapper);

      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 3);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 3);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(30) && !value.equals(20) && !value.equals(10)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItemTxRollback() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();
    Entity loadedMapper = db.load(mapper.getIdentity());
    loadedMapper.<Map<String, Integer>>getProperty("intMap").put("key3", 30);
    db.save(loadedMapper);
    db.rollback();

    var keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(20) && !value.equals(10)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItem() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();
    db.command("UPDATE " + mapper.getIdentity() + " set intMap['key2'] = 40").close();
    db.commit();

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(40)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTx() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    db.begin();
    mapper = db.save(mapper);
    db.commit();

    try {
      db.begin();
      Entity loadedMapper = db.load(mapper.getIdentity());
      loadedMapper.<Map<String, Integer>>getProperty("intMap").put("key2", 40);
      db.save(loadedMapper);
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(40)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTxRollback() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();
    Entity loadedMapper = db.load(new RecordId(mapper.getIdentity()));
    loadedMapper.<Map<String, Integer>>getProperty("intMap").put("key2", 40);
    db.save(loadedMapper);
    db.rollback();

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItem() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setProperty("intMap", map);

    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();
    db.command("UPDATE " + mapper.getIdentity() + " remove intMap = 'key2'").close();
    db.commit();

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(30)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTx() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setProperty("intMap", map);
    db.begin();
    mapper = db.save(mapper);
    db.commit();

    try {
      db.begin();
      Entity loadedMapper = db.load(mapper.getIdentity());
      loadedMapper.<Map<String, Integer>>getProperty("intMap").remove("key2");
      db.save(loadedMapper);
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getInternal().size(db), 2);
    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(30)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTxRollback() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setProperty("intMap", map);

    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();
    Entity loadedMapper = db.load(mapper.getIdentity());
    loadedMapper.<Map<String, Integer>>getProperty("intMap").remove("key2");
    db.save(loadedMapper);
    db.rollback();

    var keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(db), 3);
    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getInternal().size(db), 3);
    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20) && !value.equals(30)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemove() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();
    db.delete(db.bindToSession(mapper));
    db.commit();

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 0);

    var valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getInternal().size(db), 0);
  }

  public void testIndexMapRemoveInTx() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    db.begin();
    mapper = db.save(mapper);
    db.commit();

    try {
      db.begin();
      db.delete(db.bindToSession(mapper));
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 0);

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 0);
  }

  public void testIndexMapRemoveInTxRollback() {
    checkEmbeddedDB();

    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    db.begin();
    mapper = db.save(mapper);
    db.commit();

    db.begin();
    db.delete(db.bindToSession(mapper));
    db.rollback();

    var keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    var valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (var valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        var value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapSQL() {
    var mapper = db.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    db.begin();
    db.save(mapper);
    db.commit();

    var resultByKey =
        executeQuery("select * from Mapper where intMap containskey ?", "key1");
    Assert.assertNotNull(resultByKey);
    Assert.assertEquals(resultByKey.size(), 1);

    Assert.assertEquals(map, resultByKey.get(0).<Map<String, Integer>>getProperty("intMap"));

    var resultByValue =
        executeQuery("select * from Mapper where intMap containsvalue ?", 10);
    Assert.assertNotNull(resultByValue);
    Assert.assertEquals(resultByValue.size(), 1);

    Assert.assertEquals(map, resultByValue.get(0).<Map<String, Integer>>getProperty("intMap"));
  }
}
