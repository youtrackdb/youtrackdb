package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ORidBagAtomicUpdateTest extends BaseMemoryDatabase {

  private int topThreshold;
  private int bottomThreshold;

  @Before
  public void beforeMethod() {
    topThreshold =
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @After
  public void afterMethod() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  @Test
  public void testAddTwoNewDocuments() {
    db.begin();
    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  @Test
  public void testAddTwoNewDocumentsWithCME() throws Exception {
    db.begin();

    ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    cmeDoc = db.bindToSession(cmeDoc);
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    cmeDoc.field("v", "v234");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    generateCME(cmeDoc.getIdentity());

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  @Test
  public void testAddTwoAdditionalNewDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ODocument docThree = new ODocument();
    ODocument docFour = new ODocument();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddingDocsDontUpdateVersion() {
    db.begin();
    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();

    ridBag.add(docOne);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    final int version = rootDoc.getVersion();

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ODocument docTwo = new ODocument();
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);

    db.begin();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);
    db.rollback();
  }

  @Test
  public void testAddingDocsDontUpdateVersionInTx() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();

    ridBag.add(docOne);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    final int version = rootDoc.getVersion();

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ODocument docTwo = new ODocument();
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);

    db.begin();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);
    db.rollback();
  }

  @Test
  public void testAddTwoAdditionalNewDocumentsWithCME() throws Exception {
    db.begin();
    ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();

    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v");

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    ODocument docThree = new ODocument();
    ODocument docFour = new ODocument();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddTwoSavedDocuments() {
    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));
    ODocument docTwo = new ODocument();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
  }

  @Test
  public void testAddTwoAdditionalSavedDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ODocument docThree = new ODocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));
    ODocument docFour = new ODocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddTwoAdditionalSavedDocumentsWithCME() throws Exception {
    db.begin();
    ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();

    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThree = new ODocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));
    ODocument docFour = new ODocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    generateCME(cmeDoc.getIdentity());

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddInternalDocumentsAndSubDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docTwo = new ODocument();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();
    ODocument docThree = new ODocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFour = new ODocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThreeOne = new ODocument();
    docThreeOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThreeTwo = new ODocument();
    docThreeTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagThree = new ORidBag();
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFourOne = new ODocument();
    docFourOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFourTwo = new ODocument();
    docFourTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagFour = new ORidBag();
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddInternalDocumentsAndSubDocumentsWithCME() throws Exception {
    db.begin();
    ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docTwo = new ODocument();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();
    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v2");
    cmeDoc.save();

    ODocument docThree = new ODocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFour = new ODocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThreeOne = new ODocument();
    docThreeOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThreeTwo = new ODocument();
    docThreeTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagThree = new ORidBag();
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFourOne = new ODocument();
    docFourOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFourTwo = new ODocument();
    docFourTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagFour = new ORidBag();
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testRandomChangedInTxLevel2() {
    testRandomChangedInTx(2);
  }

  @Test
  public void testRandomChangedInTxLevel1() {
    testRandomChangedInTx(1);
  }

  private void testRandomChangedInTx(final int levels) {
    Random rnd = new Random();

    final List<Integer> amountOfAddedDocsPerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfAddedDocsAfterSavePerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfDeletedDocsPerLevel = new ArrayList<Integer>();
    Map<LevelKey, List<OIdentifiable>> addedDocPerLevel =
        new HashMap<LevelKey, List<OIdentifiable>>();

    for (int i = 0; i < levels; i++) {
      amountOfAddedDocsPerLevel.add(rnd.nextInt(5) + 10);
      amountOfAddedDocsAfterSavePerLevel.add(rnd.nextInt(5) + 5);
      amountOfDeletedDocsPerLevel.add(rnd.nextInt(5) + 5);
    }

    db.begin();
    ODocument rootDoc = new ODocument();
    createDocsForLevel(amountOfAddedDocsPerLevel, 0, levels, addedDocPerLevel, rootDoc);
    db.commit();

    addedDocPerLevel = new HashMap<LevelKey, List<OIdentifiable>>(addedDocPerLevel);

    rootDoc = db.load(rootDoc.getIdentity());
    db.begin();
    deleteDocsForLevel(db, amountOfDeletedDocsPerLevel, 0, levels, rootDoc, rnd);
    addDocsForLevel(db, amountOfAddedDocsAfterSavePerLevel, 0, levels, rootDoc);
    db.rollback();

    rootDoc = db.load(rootDoc.getIdentity());
    assertDocsAfterRollback(0, levels, addedDocPerLevel, rootDoc);
  }

  @Test
  public void testRandomChangedInTxWithCME() throws Exception {
    db.begin();
    ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Random rnd = new Random();

    final int levels = rnd.nextInt(2) + 1;
    final List<Integer> amountOfAddedDocsPerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfAddedDocsAfterSavePerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfDeletedDocsPerLevel = new ArrayList<Integer>();
    Map<LevelKey, List<OIdentifiable>> addedDocPerLevel =
        new HashMap<LevelKey, List<OIdentifiable>>();

    for (int i = 0; i < levels; i++) {
      amountOfAddedDocsPerLevel.add(rnd.nextInt(5) + 10);
      amountOfAddedDocsAfterSavePerLevel.add(rnd.nextInt(5) + 5);
      amountOfDeletedDocsPerLevel.add(rnd.nextInt(5) + 5);
    }

    db.begin();
    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    ODocument rootDoc = new ODocument();
    createDocsForLevel(amountOfAddedDocsPerLevel, 0, levels, addedDocPerLevel, rootDoc);
    db.commit();

    addedDocPerLevel = new HashMap<>(addedDocPerLevel);

    rootDoc = db.load(rootDoc.getIdentity());
    db.begin();
    deleteDocsForLevel(db, amountOfDeletedDocsPerLevel, 0, levels, rootDoc, rnd);
    addDocsForLevel(db, amountOfAddedDocsAfterSavePerLevel, 0, levels, rootDoc);

    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "vn");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    assertDocsAfterRollback(0, levels, addedDocPerLevel, rootDoc);
  }

  @Test
  public void testFromEmbeddedToSBTreeRollback() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    db.begin();
    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromEmbeddedToSBTreeTXWithCME() throws Exception {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    db.begin();
    ODocument cmeDocument = new ODocument();
    cmeDocument.field("v", "v1");
    cmeDocument.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());

    ODocument staleDocument = db.load(cmeDocument.getIdentity());
    Assert.assertNotSame(staleDocument, cmeDocument);

    db.begin();

    cmeDocument = db.bindToSession(cmeDocument);
    cmeDocument.field("v", "v234");
    cmeDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    generateCME(cmeDocument.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromEmbeddedToSBTreeWithCME() throws Exception {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    db.begin();
    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());

    var rid = document.getIdentity();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());
    try {
      generateCME(rid);
      document.save(db.getClusterNameById(db.getDefaultClusterId()));
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  private void generateCME(ORID rid) throws InterruptedException {
    var th =
        new Thread(
            () -> {
              try (var session = db.copy()) {
                session.activateOnCurrentThread();
                session.begin();
                ODocument cmeDocument = session.load(rid);

                cmeDocument.field("v", "v1");
                cmeDocument.save(session.getClusterNameById(session.getDefaultClusterId()));
                session.commit();
              }
            });
    th.start();
    th.join();
  }

  @Test
  public void testFromSBTreeToEmbeddedRollback() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    db.begin();
    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertEquals(docsToAdd.size(), 10);
    Assert.assertFalse(ridBag.isEmbedded());

    document = db.load(document.getIdentity());

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 4; i++) {
      OIdentifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertFalse(ridBag.isEmbedded());

    for (OIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromSBTreeToEmbeddedTxWithCME() throws Exception {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    db.begin();
    ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 10);
    Assert.assertFalse(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    document.field("ridBag");

    db.begin();
    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "sd");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 4; i++) {
      OIdentifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertFalse(ridBag.isEmbedded());

    for (OIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  private void createDocsForLevel(
      final List<Integer> amountOfAddedDocsPerLevel,
      int level,
      int levels,
      Map<LevelKey, List<OIdentifiable>> addedDocPerLevel,
      ODocument rootDoc) {

    int docs = amountOfAddedDocsPerLevel.get(level);

    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>();
    addedDocPerLevel.put(new LevelKey(rootDoc.getIdentity(), level), addedDocs);

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    for (int i = 0; i < docs; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));

      addedDocs.add(docToAdd.getIdentity());
      ridBag.add(docToAdd);

      if (level + 1 < levels) {
        createDocsForLevel(
            amountOfAddedDocsPerLevel, level + 1, levels, addedDocPerLevel, docToAdd);
      }
    }

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void deleteDocsForLevel(
      ODatabaseSessionInternal db,
      List<Integer> amountOfDeletedDocsPerLevel,
      int level,
      int levels,
      ODocument rootDoc,
      Random rnd) {
    rootDoc = db.bindToSession(rootDoc);
    ORidBag ridBag = rootDoc.field("ridBag");
    Iterator<OIdentifiable> iter = ridBag.iterator();
    while (iter.hasNext()) {
      OIdentifiable identifiable = iter.next();
      ODocument doc = identifiable.getRecord();
      if (level + 1 < levels) {
        deleteDocsForLevel(db, amountOfDeletedDocsPerLevel, level + 1, levels, doc, rnd);
      }
    }

    int docs = amountOfDeletedDocsPerLevel.get(level);

    int k = 0;
    Iterator<OIdentifiable> iterator = ridBag.iterator();
    while (k < docs && iterator.hasNext()) {
      iterator.next();

      if (rnd.nextBoolean()) {
        iterator.remove();
        k++;
      }

      if (!iterator.hasNext()) {
        iterator = ridBag.iterator();
      }
    }
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void addDocsForLevel(
      ODatabaseSessionInternal db,
      List<Integer> amountOfAddedDocsAfterSavePerLevel,
      int level,
      int levels,
      ODocument rootDoc) {
    rootDoc = db.bindToSession(rootDoc);
    ORidBag ridBag = rootDoc.field("ridBag");

    for (OIdentifiable identifiable : ridBag) {
      ODocument doc = identifiable.getRecord();
      if (level + 1 < levels) {
        addDocsForLevel(db, amountOfAddedDocsAfterSavePerLevel, level + 1, levels, doc);
      }
    }

    int docs = amountOfAddedDocsAfterSavePerLevel.get(level);
    for (int i = 0; i < docs; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));

      ridBag.add(docToAdd);
    }
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void assertDocsAfterRollback(
      int level,
      int levels,
      Map<LevelKey, List<OIdentifiable>> addedDocPerLevel,
      ODocument rootDoc) {
    ORidBag ridBag = rootDoc.field("ridBag");
    List<OIdentifiable> addedDocs =
        new ArrayList<OIdentifiable>(
            addedDocPerLevel.get(new LevelKey(rootDoc.getIdentity(), level)));

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    while (iterator.hasNext()) {
      ODocument doc = iterator.next().getRecord();
      if (level + 1 < levels) {
        assertDocsAfterRollback(level + 1, levels, addedDocPerLevel, doc);
      } else {
        Assert.assertNull(doc.field("ridBag"));
      }

      Assert.assertTrue(addedDocs.remove(doc));
    }

    Assert.assertTrue(addedDocs.isEmpty());
  }

  private final class LevelKey {

    private final ORID rid;
    private final int level;

    private LevelKey(ORID rid, int level) {
      this.rid = rid;
      this.level = level;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      LevelKey levelKey = (LevelKey) o;

      if (level != levelKey.level) {
        return false;
      }
      return rid.equals(levelKey.rid);
    }

    @Override
    public int hashCode() {
      int result = rid.hashCode();
      result = 31 * result + level;
      return result;
    }
  }
}
