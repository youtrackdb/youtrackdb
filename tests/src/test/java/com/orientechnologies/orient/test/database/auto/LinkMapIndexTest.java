package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
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
 * @since 22.03.12
 */
@SuppressWarnings("deprecation")
@Test
public class LinkMapIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public LinkMapIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    final YTClass linkMapIndexTestClass =
        database.getMetadata().getSchema().createClass("LinkMapIndexTestClass");
    linkMapIndexTestClass.createProperty(database, "linkMap", YTType.LINKMAP);

    linkMapIndexTestClass.createIndex(database, "mapIndexTestKey", YTClass.INDEX_TYPE.NOTUNIQUE,
        "linkMap");
    linkMapIndexTestClass.createIndex(database,
        "mapIndexTestValue", YTClass.INDEX_TYPE.NOTUNIQUE, "linkMap by value");
  }

  @AfterClass
  public void destroySchema() {
    database = createSessionInstance();
    database.getMetadata().getSchema().dropClass("LinkMapIndexTestClass");
    database.close();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.begin();
    database.command("delete from LinkMapIndexTestClass").close();
    database.commit();

    super.afterMethod();
  }

  public void testIndexMap() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        String key = (String) keyIterator.next();

        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);
    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapInTx() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    try {
      database.begin();
      Map<String, YTRID> map = new HashMap<>();

      map.put("key1", docOne.getIdentity());
      map.put("key2", docTwo.getIdentity());

      final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
      document.field("linkMap", map);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        String key = (String) keyIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOne() {
    checkEmbeddedDB();

    database.begin();

    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> mapOne = new HashMap<>();

    mapOne.put("key1", docOne.getIdentity());
    mapOne.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", mapOne);
    document.save();

    final Map<String, YTRID> mapTwo = new HashMap<>();
    mapTwo.put("key2", docOne.getIdentity());
    mapTwo.put("key3", docThree.getIdentity());

    document.field("linkMap", mapTwo);
    document.save();

    database.commit();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTx() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    try {
      final Map<String, YTRID> mapTwo = new HashMap<>();

      mapTwo.put("key3", docOne.getIdentity());
      mapTwo.put("key2", docTwo.getIdentity());

      final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
      document.field("linkMap", mapTwo);
      document.save();

      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> mapOne = new HashMap<>();

    mapOne.put("key1", docOne.getIdentity());
    mapOne.put("key2", docTwo.getIdentity());

    YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", mapOne);
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    final Map<String, YTRID> mapTwo = new HashMap<>();

    mapTwo.put("key3", docTwo.getIdentity());
    mapTwo.put("key2", docThree.getIdentity());

    document.field("linkMap", mapTwo);
    document.save();
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key1")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docTwo.getIdentity())
            && !value.equals(docOne.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItem() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    database.begin();
    database
        .command(
            "UPDATE " + document.getIdentity() + " set linkMap['key3'] = " + docThree.getIdentity())
        .close();
    database.commit();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 3);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItemTx() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    try {
      database.begin();
      final YTEntityImpl loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Map<String, YTRID>>field("linkMap").put("key3", docThree.getIdentity());
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 3);

    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();

        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 3);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItemTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    database.begin();
    final YTEntityImpl loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Map<String, YTRID>>field("linkMap").put("key3", docThree.getIdentity());
    loadedDocument.save();
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docTwo.getIdentity())
            && !value.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItem() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    database.begin();
    database
        .command(
            "UPDATE " + document.getIdentity() + " set linkMap['key2'] = " + docThree.getIdentity())
        .close();
    database.commit();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);
    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTx() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    try {
      database.begin();
      final YTEntityImpl loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Map<String, YTRID>>field("linkMap").put("key2", docThree.getIdentity());
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);
    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    database.begin();
    final YTEntityImpl loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Map<String, YTRID>>field("linkMap").put("key2", docThree.getIdentity());
    loadedDocument.save();
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItem() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());
    map.put("key3", docThree.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    database.begin();
    database.command("UPDATE " + document.getIdentity() + " remove linkMap = 'key2'").close();
    database.commit();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTx() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());
    map.put("key3", docThree.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    try {
      database.begin();
      final YTEntityImpl loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Map<String, YTRID>>field("linkMap").remove("key2");
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());
    map.put("key3", docThree.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    database.begin();
    final YTEntityImpl loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Map<String, YTRID>>field("linkMap").remove("key2");
    loadedDocument.save();
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 3);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        String key = (String) keyIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 3);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemove() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docThree = new YTEntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    database.begin();
    database.bindToSession(document).delete();
    database.commit();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 0);

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 0);
  }

  public void testIndexMapRemoveInTx() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    try {
      database.begin();
      database.bindToSession(document).delete();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 0);

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 0);
  }

  public void testIndexMapRemoveInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    database.begin();
    database.bindToSession(document).delete();
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(database), 2);

    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(database), 2);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        YTIdentifiable value = (YTIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapSQL() {

    database.begin();
    final YTEntityImpl docOne = new YTEntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final YTEntityImpl docTwo = new YTEntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, YTRID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    YTEntityImpl document = new YTEntityImpl("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    database.commit();

    final List<YTEntityImpl> resultByKey =
        database.query(
            new OSQLSynchQuery<YTEntityImpl>(
                "select * from LinkMapIndexTestClass where linkMap containskey ?"),
            "key1");
    Assert.assertNotNull(resultByKey);
    Assert.assertEquals(resultByKey.size(), 1);

    document = database.bindToSession(document);
    Assert.assertEquals(map, document.field("linkMap"));

    final List<YTEntityImpl> resultByValue =
        executeQuery(
            "select * from LinkMapIndexTestClass where linkMap  containsvalue ?",
            docOne.getIdentity());
    Assert.assertNotNull(resultByValue);
    Assert.assertEquals(resultByValue.size(), 1);

    Assert.assertEquals(map, document.field("linkMap"));
  }
}
