package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 1/30/14
 */
@Test
public class LinkBagIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public LinkBagIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    final OClass ridBagIndexTestClass =
        database.getMetadata().getSchema().createClass("RidBagIndexTestClass");

    ridBagIndexTestClass.createProperty(database, "ridBag", OType.LINKBAG);

    ridBagIndexTestClass.createIndex(database, "ridBagIndex", OClass.INDEX_TYPE.NOTUNIQUE,
        "ridBag");

    database.close();
  }

  @AfterClass
  public void destroySchema() {
    if (database.isClosed()) {
      database = acquireSession();
    }

    database.getMetadata().getSchema().dropClass("RidBagIndexTestClass");
    database.close();
  }

  @AfterMethod
  public void afterMethod() {
    database.begin();
    database.command("DELETE FROM RidBagIndexTestClass").close();
    database.commit();

    OResultSet result = database.query("select from RidBagIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (!database.getStorage().isRemote()) {
      final OIndex index = getIndex("ridBagIndex");
      Assert.assertEquals(index.getInternal().size(database), 0);
    }
  }

  public void testIndexRidBag() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);

    document.save();
    database.commit();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagInTx() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));
    try {
      final ODocument document = new ODocument("RidBagIndexTestClass");
      final ORidBag ridBag = new ORidBag(database);
      ridBag.add(docOne);
      ridBag.add(docTwo);

      document.field("ridBag", ridBag);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdate() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBagOne = new ORidBag(database);
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);

    document.save();
    database.commit();

    database.begin();
    final ORidBag ridBagTwo = new ORidBag(database);
    ridBagTwo.add(docOne);
    ridBagTwo.add(docThree);

    document = database.bindToSession(document);
    document.field("ridBag", ridBagTwo);

    database.bindToSession(document).save();
    database.commit();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateInTx() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBagOne = new ORidBag(database);
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);

    document.save();
    database.commit();

    try {
      database.begin();

      document = database.bindToSession(document);
      final ORidBag ridBagTwo = new ORidBag(database);
      ridBagTwo.add(docOne);
      ridBagTwo.add(docThree);

      document.field("ridBag", ridBagTwo);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ORidBag ridBagOne = new ORidBag(database);
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    ODocument document = new ODocument("RidBagIndexTestClass");
    document.field("ridBag", ridBagOne);
    document.save();
    Assert.assertTrue(database.commit());

    database.begin();
    document = database.bindToSession(document);
    final ORidBag ridBagTwo = new ORidBag(database);
    ridBagTwo.add(docOne);
    ridBagTwo.add(docThree);

    document.field("ridBag", ridBagTwo);
    document.save();
    database.rollback();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateAddItem() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);

    document.save();
    database.commit();

    database.begin();
    database
        .command(
            "UPDATE "
                + document.getIdentity()
                + " set ridBag = ridBag || "
                + docThree.getIdentity())
        .close();
    database.commit();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 3);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateAddItemInTx() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);

    document.save();
    database.commit();

    try {
      database.begin();
      docThree = database.bindToSession(docThree);
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<ORidBag>field("ridBag").add(docThree);
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 3);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);

    document.save();
    database.commit();

    database.begin();
    docThree = database.bindToSession(docThree);
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<ORidBag>field("ridBag").add(docThree);
    loadedDocument.save();
    database.rollback();

    final OIndex index = getIndex("ridBagIndex");

    Assert.assertEquals(index.getInternal().size(database), 2);
    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);

    document.save();
    database.commit();

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<ORidBag>field("ridBag").remove(docTwo);
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("ridBagIndex");

    Assert.assertEquals(index.getInternal().size(database), 1);
    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);
    document.save();
    database.commit();

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<ORidBag>field("ridBag").remove(docTwo);
    loadedDocument.save();
    database.rollback();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItem() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    database.commit();

    //noinspection deprecation
    database.begin();
    database
        .command("UPDATE " + document.getIdentity() + " remove ridBag = " + docTwo.getIdentity())
        .close();
    database.commit();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 1);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagRemove() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");

    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    document.delete();
    database.commit();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testIndexRidBagRemoveInTx() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");

    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
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

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testIndexRidBagRemoveInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag(database);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    database.commit();

    database.begin();
    database.bindToSession(document).delete();
    database.rollback();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagSQL() {
    database.begin();
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBagOne = new ORidBag(database);
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);
    document.save();

    document = new ODocument("RidBagIndexTestClass");
    ORidBag ridBag = new ORidBag(database);
    ridBag.add(docThree);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    database.commit();

    OResultSet result =
        database.query(
            "select * from RidBagIndexTestClass where ridBag contains ?", docOne.getIdentity());
    OResult res = result.next();

    List<OIdentifiable> listResult = new ArrayList<>();
    for (OIdentifiable identifiable : res.<ORidBag>getProperty("ridBag")) {
      listResult.add(identifiable);
    }
    result.close();

    Assert.assertEquals(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity()), listResult);
  }
}
