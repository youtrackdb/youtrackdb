package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
public class MapIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public MapIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    if (database.getMetadata().getSchema().existsClass("Mapper")) {
      database.getMetadata().getSchema().dropClass("Mapper");
    }

    final YTClass mapper = database.getMetadata().getSchema().createClass("Mapper");
    mapper.createProperty(database, "id", YTType.STRING);
    mapper.createProperty(database, "intMap", YTType.EMBEDDEDMAP, YTType.INTEGER);

    mapper.createIndex(database, "mapIndexTestKey", YTClass.INDEX_TYPE.NOTUNIQUE, "intMap");
    mapper.createIndex(database, "mapIndexTestValue", YTClass.INDEX_TYPE.NOTUNIQUE,
        "intMap by value");

    final YTClass movie = database.getMetadata().getSchema().createClass("MapIndexTestMovie");
    movie.createProperty(database, "title", YTType.STRING);
    movie.createProperty(database, "thumbs", YTType.EMBEDDEDMAP, YTType.INTEGER);

    movie.createIndex(database, "indexForMap", YTClass.INDEX_TYPE.NOTUNIQUE, "thumbs by key");
  }

  @AfterClass
  public void destroySchema() {
    database = createSessionInstance();
    database.getMetadata().getSchema().dropClass("Mapper");
    database.getMetadata().getSchema().dropClass("MapIndexTestMovie");
    database.close();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.begin();
    database.command("delete from Mapper").close();
    database.command("delete from MapIndexTestMovie").close();
    database.commit();

    super.afterMethod();
  }

  public void testIndexMap() {
    checkEmbeddedDB();

    final YTEntity mapper = database.newEntity("Mapper");
    final Map<String, Integer> map = new HashMap<>();
    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    database.begin();
    database.save(mapper);
    database.commit();

    final OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 2);
    try (final Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      final Iterator<Object> keyIterator = keyStream.iterator();
      while (keyIterator.hasNext()) {
        final String key = (String) keyIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);
    try (final Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      final Iterator<Object> valuesIterator = valueStream.iterator();
      while (valuesIterator.hasNext()) {
        final Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapInTx() {
    checkEmbeddedDB();

    try {
      database.begin();
      final YTEntity mapper = database.newEntity("Mapper");
      Map<String, Integer> map = new HashMap<>();

      map.put("key1", 10);
      map.put("key2", 20);

      mapper.setProperty("intMap", map);
      database.save(mapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(database), 2);
    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOne() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> mapOne = new HashMap<>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setProperty("intMap", mapOne);
    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();

    mapper = database.bindToSession(mapper);
    final Map<String, Integer> mapTwo = new HashMap<>();

    mapTwo.put("key3", 30);
    mapTwo.put("key2", 20);

    mapper.setProperty("intMap", mapTwo);

    database.save(mapper);
    database.commit();

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(30) && !value.equals(20)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTx() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> mapOne = new HashMap<>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setProperty("intMap", mapOne);
    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();
    try {
      final Map<String, Integer> mapTwo = new HashMap<>();

      mapTwo.put("key3", 30);
      mapTwo.put("key2", 20);

      mapper = database.bindToSession(mapper);
      mapper.setProperty("intMap", mapTwo);
      database.save(mapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(30) && !value.equals(20)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTxRollback() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> mapOne = new HashMap<>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setProperty("intMap", mapOne);
    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();
    final Map<String, Integer> mapTwo = new HashMap<>();

    mapTwo.put("key3", 30);
    mapTwo.put("key2", 20);

    mapper = database.bindToSession(mapper);
    mapper.setProperty("intMap", mapTwo);
    database.save(mapper);
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key1")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItem() {
    checkEmbeddedDB();

    database.begin();
    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    mapper = database.save(mapper);
    database.commit();

    database.begin();
    database.command("UPDATE " + mapper.getIdentity() + " set intMap['key3'] = 30").close();
    database.commit();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 3);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(30) && !value.equals(20) && !value.equals(10)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItemTx() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    database.begin();
    mapper = database.save(mapper);
    database.commit();

    try {
      database.begin();
      YTEntity loadedMapper = database.load(mapper.getIdentity());
      loadedMapper.<Map<String, Integer>>getProperty("intMap").put("key3", 30);
      database.save(loadedMapper);

      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 3);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(30) && !value.equals(20) && !value.equals(10)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItemTxRollback() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();
    YTEntity loadedMapper = database.load(mapper.getIdentity());
    loadedMapper.<Map<String, Integer>>getProperty("intMap").put("key3", 30);
    database.save(loadedMapper);
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(20) && !value.equals(10)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItem() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();
    database.command("UPDATE " + mapper.getIdentity() + " set intMap['key2'] = 40").close();
    database.commit();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(40)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTx() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    database.begin();
    mapper = database.save(mapper);
    database.commit();

    try {
      database.begin();
      YTEntity loadedMapper = database.load(mapper.getIdentity());
      loadedMapper.<Map<String, Integer>>getProperty("intMap").put("key2", 40);
      database.save(loadedMapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(40)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTxRollback() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();
    YTEntity loadedMapper = database.load(new YTRecordId(mapper.getIdentity()));
    loadedMapper.<Map<String, Integer>>getProperty("intMap").put("key2", 40);
    database.save(loadedMapper);
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItem() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setProperty("intMap", map);

    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();
    database.command("UPDATE " + mapper.getIdentity() + " remove intMap = 'key2'").close();
    database.commit();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(30)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTx() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setProperty("intMap", map);
    database.begin();
    mapper = database.save(mapper);
    database.commit();

    try {
      database.begin();
      YTEntity loadedMapper = database.load(mapper.getIdentity());
      loadedMapper.<Map<String, Integer>>getProperty("intMap").remove("key2");
      database.save(loadedMapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getInternal().size(database), 2);
    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(30)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTxRollback() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setProperty("intMap", map);

    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();
    YTEntity loadedMapper = database.load(mapper.getIdentity());
    loadedMapper.<Map<String, Integer>>getProperty("intMap").remove("key2");
    database.save(loadedMapper);
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(database), 3);
    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getInternal().size(database), 3);
    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20) && !value.equals(30)) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemove() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);
    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();
    database.delete(database.bindToSession(mapper));
    database.commit();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 0);

    OIndex valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getInternal().size(database), 0);
  }

  public void testIndexMapRemoveInTx() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    database.begin();
    mapper = database.save(mapper);
    database.commit();

    try {
      database.begin();
      database.delete(database.bindToSession(mapper));
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 0);

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 0);
  }

  public void testIndexMapRemoveInTxRollback() {
    checkEmbeddedDB();

    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    database.begin();
    mapper = database.save(mapper);
    database.commit();

    database.begin();
    database.delete(database.bindToSession(mapper));
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndex.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndex.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Integer value = (Integer) valuesIterator.next();
        if (!value.equals(10) && !value.equals(20)) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapSQL() {
    YTEntity mapper = database.newEntity("Mapper");
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setProperty("intMap", map);

    database.begin();
    database.save(mapper);
    database.commit();

    final List<YTDocument> resultByKey =
        executeQuery("select * from Mapper where intMap containskey ?", "key1");
    Assert.assertNotNull(resultByKey);
    Assert.assertEquals(resultByKey.size(), 1);

    Assert.assertEquals(map, resultByKey.get(0).<Map<String, Integer>>getProperty("intMap"));

    final List<YTDocument> resultByValue =
        executeQuery("select * from Mapper where intMap containsvalue ?", 10);
    Assert.assertNotNull(resultByValue);
    Assert.assertEquals(resultByValue.size(), 1);

    Assert.assertEquals(map, resultByValue.get(0).<Map<String, Integer>>getProperty("intMap"));
  }
}
