package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
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
public class LinkMapIndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public LinkMapIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    final SchemaClass linkMapIndexTestClass =
        db.getMetadata().getSchema().createClass("LinkMapIndexTestClass");
    linkMapIndexTestClass.createProperty(db, "linkMap", PropertyType.LINKMAP);

    linkMapIndexTestClass.createIndex(db, "mapIndexTestKey", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "linkMap");
    linkMapIndexTestClass.createIndex(db,
        "mapIndexTestValue", SchemaClass.INDEX_TYPE.NOTUNIQUE, "linkMap by value");
  }

  @AfterClass
  public void destroySchema() {
    db = createSessionInstance();
    db.getMetadata().getSchema().dropClass("LinkMapIndexTestClass");
    db.close();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    db.begin();
    db.command("delete from LinkMapIndexTestClass").close();
    db.commit();

    super.afterMethod();
  }

  public void testIndexMap() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);
    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();
    db.commit();

    try {
      db.begin();
      Map<String, RID> map = new HashMap<>();

      map.put("key1", docOne.getIdentity());
      map.put("key2", docTwo.getIdentity());

      final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
      document.field("linkMap", map);
      document.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOne() {
    checkEmbeddedDB();

    db.begin();

    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> mapOne = new HashMap<>();

    mapOne.put("key1", docOne.getIdentity());
    mapOne.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", mapOne);
    document.save();

    final Map<String, RID> mapTwo = new HashMap<>();
    mapTwo.put("key2", docOne.getIdentity());
    mapTwo.put("key3", docThree.getIdentity());

    document.field("linkMap", mapTwo);
    document.save();

    db.commit();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    try {
      final Map<String, RID> mapTwo = new HashMap<>();

      mapTwo.put("key3", docOne.getIdentity());
      mapTwo.put("key2", docTwo.getIdentity());

      final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
      document.field("linkMap", mapTwo);
      document.save();

      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> mapOne = new HashMap<>();

    mapOne.put("key1", docOne.getIdentity());
    mapOne.put("key2", docTwo.getIdentity());

    EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", mapOne);
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    final Map<String, RID> mapTwo = new HashMap<>();

    mapTwo.put("key3", docTwo.getIdentity());
    mapTwo.put("key2", docThree.getIdentity());

    document.field("linkMap", mapTwo);
    document.save();
    db.rollback();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docTwo.getIdentity())
            && !value.equals(docOne.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItem() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    db.begin();
    db
        .command(
            "UPDATE " + document.getIdentity() + " set linkMap['key3'] = " + docThree.getIdentity())
        .close();
    db.commit();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 3);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 3);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    try {
      db.begin();
      final EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<Map<String, RID>>field("linkMap").put("key3", docThree.getIdentity());
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 3);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 3);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    db.begin();
    final EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<Map<String, RID>>field("linkMap").put("key3", docThree.getIdentity());
    loadedDocument.save();
    db.rollback();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docTwo.getIdentity())
            && !value.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItem() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    db.begin();
    db
        .command(
            "UPDATE " + document.getIdentity() + " set linkMap['key2'] = " + docThree.getIdentity())
        .close();
    db.commit();

    final Index keyIndexMap = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);
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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    try {
      db.begin();
      final EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<Map<String, RID>>field("linkMap").put("key2", docThree.getIdentity());
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);
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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    db.begin();
    final EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<Map<String, RID>>field("linkMap").put("key2", docThree.getIdentity());
    loadedDocument.save();
    db.rollback();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItem() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());
    map.put("key3", docThree.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    db.begin();
    db.command("UPDATE " + document.getIdentity() + " remove linkMap = 'key2'").close();
    db.commit();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());
    map.put("key3", docThree.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    try {
      db.begin();
      final EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<Map<String, RID>>field("linkMap").remove("key2");
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());
    map.put("key3", docThree.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    db.begin();
    final EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<Map<String, RID>>field("linkMap").remove("key2");
    loadedDocument.save();
    db.rollback();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 3);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 3);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    db.begin();
    db.bindToSession(document).delete();
    db.commit();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 0);

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 0);
  }

  public void testIndexMapRemoveInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    try {
      db.begin();
      db.bindToSession(document).delete();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 0);

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 0);
  }

  public void testIndexMapRemoveInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    db.begin();
    db.bindToSession(document).delete();
    db.rollback();

    final Index keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(db), 2);

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

    final Index valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(db), 2);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        Identifiable value = (Identifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapSQL() {

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    Map<String, RID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    EntityImpl document = ((EntityImpl) db.newEntity("LinkMapIndexTestClass"));
    document.field("linkMap", map);
    document.save();
    db.commit();

    final List<EntityImpl> resultByKey =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select * from LinkMapIndexTestClass where linkMap containskey ?"),
            "key1");
    Assert.assertNotNull(resultByKey);
    Assert.assertEquals(resultByKey.size(), 1);

    document = db.bindToSession(document);
    Assert.assertEquals(map, document.field("linkMap"));

    final List<EntityImpl> resultByValue =
        executeQuery(
            "select * from LinkMapIndexTestClass where linkMap  containsvalue ?",
            docOne.getIdentity());
    Assert.assertNotNull(resultByValue);
    Assert.assertEquals(resultByValue.size(), 1);

    Assert.assertEquals(map, document.field("linkMap"));
  }
}
