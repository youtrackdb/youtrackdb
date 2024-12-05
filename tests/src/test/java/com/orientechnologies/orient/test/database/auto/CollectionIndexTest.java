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
package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
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
public class CollectionIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public CollectionIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    if (database.getMetadata().getSchema().existsClass("Collector")) {
      database.getMetadata().getSchema().dropClass("Collector");
    }
    final YTClass collector = database.createClass("Collector");
    collector.createProperty(database, "id", YTType.STRING);
    collector
        .createProperty(database, "stringCollection", YTType.EMBEDDEDLIST, YTType.STRING)
        .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.begin();
    database.command("delete from Collector").close();
    database.commit();

    super.afterMethod();
  }

  public void testIndexCollection() {
    checkEmbeddedDB();

    database.begin();
    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));

    database.save(collector);
    database.commit();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

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
      database.begin();
      Entity collector = database.newEntity("Collector");
      collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
      database.save(collector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

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

    database.begin();
    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    collector.setProperty("stringCollection", Arrays.asList("spam", "bacon"));
    database.save(collector);
    database.commit();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

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

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    database.begin();
    collector = database.save(collector);
    database.commit();
    try {
      database.begin();
      collector = database.bindToSession(collector);
      collector.setProperty("stringCollection", Arrays.asList("spam", "bacon"));
      database.save(collector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(database), 2);
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

    database.begin();
    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    database.commit();

    database.begin();
    collector = database.bindToSession(collector);
    collector.setProperty("stringCollection", Arrays.asList("spam", "bacon"));
    database.save(collector);
    database.rollback();

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(database), 2);

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

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));

    database.begin();
    collector = database.save(collector);
    database.commit();

    database.begin();
    database
        .command(
            "UPDATE "
                + collector.getIdentity()
                + " set stringCollection = stringCollection || 'cookies'")
        .close();
    database.commit();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(database), 3);

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

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", new ArrayList<>(Arrays.asList("spam", "eggs")));
    database.begin();
    collector = database.save(collector);
    database.commit();

    try {
      database.begin();
      Entity loadedCollector = database.load(collector.getIdentity());
      loadedCollector.<List<String>>getProperty("stringCollection").add("cookies");
      database.save(loadedCollector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(database), 3);

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

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", new ArrayList<>(Arrays.asList("spam", "eggs")));
    database.begin();
    collector = database.save(collector);
    database.commit();

    database.begin();
    Entity loadedCollector = database.load(collector.getIdentity());
    loadedCollector.<List<String>>getProperty("stringCollection").add("cookies");
    database.save(loadedCollector);
    database.rollback();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

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

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", new ArrayList<>(Arrays.asList("spam", "eggs")));
    database.begin();
    collector = database.save(collector);
    database.commit();

    try {
      database.begin();
      Entity loadedCollector = database.load(collector.getIdentity());
      loadedCollector.<List<String>>getProperty("stringCollection").remove("spam");
      database.save(loadedCollector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(database), 1);

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

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", new ArrayList<>(Arrays.asList("spam", "eggs")));
    database.begin();
    collector = database.save(collector);
    database.commit();

    database.begin();
    Entity loadedCollector = database.load(collector.getIdentity());
    loadedCollector.<List<String>>getProperty("stringCollection").remove("spam");
    database.save(loadedCollector);
    database.rollback();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

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

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    database.begin();
    collector = database.save(collector);
    database.commit();

    database.begin();
    database
        .command("UPDATE " + collector.getIdentity() + " remove stringCollection = 'spam'")
        .close();
    database.commit();

    final OIndex index = getIndex("Collector.stringCollection");

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

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    database.begin();
    collector = database.save(collector);
    database.delete(collector);
    database.commit();

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testIndexCollectionRemoveInTx() {
    checkEmbeddedDB();

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    database.begin();
    collector = database.save(collector);
    database.commit();
    try {
      database.begin();
      database.delete(database.bindToSession(collector));
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testIndexCollectionRemoveInTxRollback() {
    checkEmbeddedDB();

    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));
    database.begin();
    collector = database.save(collector);
    database.commit();

    database.begin();
    database.delete(database.bindToSession(collector));
    database.rollback();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(database), 2);

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
    Entity collector = database.newEntity("Collector");
    collector.setProperty("stringCollection", Arrays.asList("spam", "eggs"));

    database.begin();
    database.save(collector);
    database.commit();

    List<EntityImpl> result =
        executeQuery("select * from Collector where stringCollection contains ?", "eggs");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(
        Arrays.asList("spam", "eggs"), result.get(0).getProperty("stringCollection"));
  }
}
