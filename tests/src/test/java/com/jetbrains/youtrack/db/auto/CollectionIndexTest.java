/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class CollectionIndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public CollectionIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    if (db.getMetadata().getSchema().existsClass("Collector")) {
      db.getMetadata().getSchema().dropClass("Collector");
    }
    final SchemaClass collector = db.createClass("Collector");
    collector.createProperty(db, "id", PropertyType.STRING);
    collector
        .createProperty(db, "stringCollection", PropertyType.EMBEDDEDLIST,
            PropertyType.STRING)
        .createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    db.begin();
    db.command("delete from Collector").close();
    db.commit();

    super.afterMethod();
  }

  public void testIndexCollection() {
    checkEmbeddedDB();

    db.begin();
    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));

    db.save(collector);
    db.commit();

    final Index index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionInTx() {
    checkEmbeddedDB();

    try {
      db.begin();
      Entity collector = db.newEntity("Collector");
      collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
      db.save(collector);
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();
      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdate() {
    checkEmbeddedDB();

    db.begin();
    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    collector = db.save(collector);
    collector.setProperty("stringCollection", Arrays.asList("spam", "bacon"));
    db.save(collector);
    db.commit();

    final Index index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("bacon")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateInTx() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    db.begin();
    collector = db.save(collector);
    db.commit();
    try {
      db.begin();
      collector = db.bindToSession(collector);
      collector.setProperty("stringCollection", Arrays.asList("spam", "bacon"));
      db.save(collector);
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(db), 2);
    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("bacon")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    collector = db.save(collector);
    db.commit();

    db.begin();
    collector = db.bindToSession(collector);
    collector.setProperty("stringCollection", Arrays.asList("spam", "bacon"));
    db.save(collector);
    db.rollback();

    final Index index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItem() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));

    db.begin();
    collector = db.save(collector);
    db.commit();

    db.begin();
    db
        .command(
            "UPDATE "
                + collector.getIdentity()
                + " set stringCollection = stringCollection || 'cookies'")
        .close();
    db.commit();

    final Index index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(db), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs") && !key.equals("cookies")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItemInTx() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", new ArrayList<>(Arrays.asList("spam", "eggs")));
    db.begin();
    collector = db.save(collector);
    db.commit();

    try {
      db.begin();
      Entity loadedCollector = db.load(collector.getIdentity());
      loadedCollector.<List<String>>getProperty("stringCollection").add("cookies");
      db.save(loadedCollector);
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(db), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs") && !key.equals("cookies")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", new ArrayList<>(Arrays.asList("spam", "eggs")));
    db.begin();
    collector = db.save(collector);
    db.commit();

    db.begin();
    Entity loadedCollector = db.load(collector.getIdentity());
    loadedCollector.<List<String>>getProperty("stringCollection").add("cookies");
    db.save(loadedCollector);
    db.rollback();

    final Index index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", new ArrayList<>(Arrays.asList("spam", "eggs")));
    db.begin();
    collector = db.save(collector);
    db.commit();

    try {
      db.begin();
      Entity loadedCollector = db.load(collector.getIdentity());
      loadedCollector.<List<String>>getProperty("stringCollection").remove("spam");
      db.save(loadedCollector);
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(db), 1);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", new ArrayList<>(Arrays.asList("spam", "eggs")));
    db.begin();
    collector = db.save(collector);
    db.commit();

    db.begin();
    Entity loadedCollector = db.load(collector.getIdentity());
    loadedCollector.<List<String>>getProperty("stringCollection").remove("spam");
    db.save(loadedCollector);
    db.rollback();

    final Index index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItem() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    db.begin();
    collector = db.save(collector);
    db.commit();

    db.begin();
    db
        .command("UPDATE " + collector.getIdentity() + " remove stringCollection = 'spam'")
        .close();
    db.commit();

    final Index index = getIndex("Collector.stringCollection");

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionRemove() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    db.begin();
    collector = db.save(collector);
    db.delete(collector);
    db.commit();

    final Index index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexCollectionRemoveInTx() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    db.begin();
    collector = db.save(collector);
    db.commit();
    try {
      db.begin();
      db.delete(db.bindToSession(collector));
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexCollectionRemoveInTxRollback() {
    checkEmbeddedDB();

    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    db.begin();
    collector = db.save(collector);
    db.commit();

    db.begin();
    db.delete(db.bindToSession(collector));
    db.rollback();

    final Index index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionSQL() {
    Entity collector = db.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));

    db.begin();
    db.save(collector);
    db.commit();

    List<EntityImpl> result =
        executeQuery("select * from Collector where stringCollection contains ?", "eggs");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(
        Arrays.asList("spam", "eggs"), result.get(0).getProperty("stringCollection"));
  }
}
