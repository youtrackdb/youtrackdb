package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.YTConcurrentModificationException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTDocument;
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

public class ORidBagAtomicUpdateTest extends DBTestBase {

  private int topThreshold;
  private int bottomThreshold;

  @Before
  public void beforeMethod() {
    topThreshold =
        YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @After
  public void afterMethod() {
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  @Test
  public void testAddTwoNewDocuments() {
    db.begin();
    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    YTDocument docOne = new YTDocument();
    YTDocument docTwo = new YTDocument();

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

    YTDocument cmeDoc = new YTDocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
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

    YTDocument docOne = new YTDocument();
    YTDocument docTwo = new YTDocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    generateCME(cmeDoc.getIdentity());

    try {
      db.commit();
      Assert.fail();
    } catch (YTConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  @Test
  public void testAddTwoAdditionalNewDocuments() {
    db.begin();

    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    YTDocument docOne = new YTDocument();
    YTDocument docTwo = new YTDocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    YTDocument docThree = new YTDocument();
    YTDocument docFour = new YTDocument();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    Iterator<YTIdentifiable> iterator = ridBag.iterator();
    List<YTIdentifiable> addedDocs = new ArrayList<YTIdentifiable>(Arrays.asList(docOne, docTwo));

    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddingDocsDontUpdateVersion() {
    db.begin();
    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    YTDocument docOne = new YTDocument();

    ridBag.add(docOne);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    final int version = rootDoc.getVersion();

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    YTDocument docTwo = new YTDocument();
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

    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    YTDocument docOne = new YTDocument();

    ridBag.add(docOne);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    final int version = rootDoc.getVersion();

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    YTDocument docTwo = new YTDocument();
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
    YTDocument cmeDoc = new YTDocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    YTDocument docOne = new YTDocument();
    YTDocument docTwo = new YTDocument();

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

    YTDocument docThree = new YTDocument();
    YTDocument docFour = new YTDocument();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (YTConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    Iterator<YTIdentifiable> iterator = ridBag.iterator();
    List<YTIdentifiable> addedDocs = new ArrayList<YTIdentifiable>(Arrays.asList(docOne, docTwo));

    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddTwoSavedDocuments() {
    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();

    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    YTDocument docOne = new YTDocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));
    YTDocument docTwo = new YTDocument();
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

    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    YTDocument docOne = new YTDocument();
    YTDocument docTwo = new YTDocument();

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

    YTDocument docThree = new YTDocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));
    YTDocument docFour = new YTDocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    List<YTIdentifiable> addedDocs = new ArrayList<YTIdentifiable>(Arrays.asList(docOne, docTwo));

    Iterator<YTIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddTwoAdditionalSavedDocumentsWithCME() throws Exception {
    db.begin();
    YTDocument cmeDoc = new YTDocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    YTDocument docOne = new YTDocument();
    YTDocument docTwo = new YTDocument();

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

    YTDocument docThree = new YTDocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));
    YTDocument docFour = new YTDocument();
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
    } catch (YTConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    List<YTIdentifiable> addedDocs = new ArrayList<YTIdentifiable>(Arrays.asList(docOne, docTwo));

    Iterator<YTIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddInternalDocumentsAndSubDocuments() {
    db.begin();

    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    YTDocument docOne = new YTDocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docTwo = new YTDocument();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();
    YTDocument docThree = new YTDocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docFour = new YTDocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docThreeOne = new YTDocument();
    docThreeOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docThreeTwo = new YTDocument();
    docThreeTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagThree = new ORidBag(db);
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docFourOne = new YTDocument();
    docFourOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docFourTwo = new YTDocument();
    docFourTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagFour = new ORidBag(db);
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
    List<YTIdentifiable> addedDocs = new ArrayList<YTIdentifiable>(Arrays.asList(docOne, docTwo));

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Iterator<YTIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddInternalDocumentsAndSubDocumentsWithCME() throws Exception {
    db.begin();
    YTDocument cmeDoc = new YTDocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    YTDocument rootDoc = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    YTDocument docOne = new YTDocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docTwo = new YTDocument();
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

    YTDocument docThree = new YTDocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docFour = new YTDocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docThreeOne = new YTDocument();
    docThreeOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docThreeTwo = new YTDocument();
    docThreeTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagThree = new ORidBag(db);
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docFourOne = new YTDocument();
    docFourOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    YTDocument docFourTwo = new YTDocument();
    docFourTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagFour = new ORidBag(db);
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (YTConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
    List<YTIdentifiable> addedDocs = new ArrayList<YTIdentifiable>(Arrays.asList(docOne, docTwo));

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Iterator<YTIdentifiable> iterator = ridBag.iterator();
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
    Map<LevelKey, List<YTIdentifiable>> addedDocPerLevel =
        new HashMap<LevelKey, List<YTIdentifiable>>();

    for (int i = 0; i < levels; i++) {
      amountOfAddedDocsPerLevel.add(rnd.nextInt(5) + 10);
      amountOfAddedDocsAfterSavePerLevel.add(rnd.nextInt(5) + 5);
      amountOfDeletedDocsPerLevel.add(rnd.nextInt(5) + 5);
    }

    db.begin();
    YTDocument rootDoc = new YTDocument();
    createDocsForLevel(amountOfAddedDocsPerLevel, 0, levels, addedDocPerLevel, rootDoc);
    db.commit();

    addedDocPerLevel = new HashMap<LevelKey, List<YTIdentifiable>>(addedDocPerLevel);

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
    YTDocument cmeDoc = new YTDocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Random rnd = new Random();

    final int levels = rnd.nextInt(2) + 1;
    final List<Integer> amountOfAddedDocsPerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfAddedDocsAfterSavePerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfDeletedDocsPerLevel = new ArrayList<Integer>();
    Map<LevelKey, List<YTIdentifiable>> addedDocPerLevel =
        new HashMap<LevelKey, List<YTIdentifiable>>();

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
    YTDocument rootDoc = new YTDocument();
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
    } catch (YTConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    assertDocsAfterRollback(0, levels, addedDocPerLevel, rootDoc);
  }

  @Test
  public void testFromEmbeddedToSBTreeRollback() {
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<YTIdentifiable> docsToAdd = new ArrayList<YTIdentifiable>();

    db.begin();
    YTDocument document = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      YTDocument docToAdd = new YTDocument();
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
      YTDocument docToAdd = new YTDocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (YTIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromEmbeddedToSBTreeTXWithCME() throws Exception {
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    db.begin();
    YTDocument cmeDocument = new YTDocument();
    cmeDocument.field("v", "v1");
    cmeDocument.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    List<YTIdentifiable> docsToAdd = new ArrayList<YTIdentifiable>();

    YTDocument document = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 3; i++) {
      YTDocument docToAdd = new YTDocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());

    YTDocument staleDocument = db.load(cmeDocument.getIdentity());
    Assert.assertNotSame(staleDocument, cmeDocument);

    db.begin();

    cmeDocument = db.bindToSession(cmeDocument);
    cmeDocument.field("v", "v234");
    cmeDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      YTDocument docToAdd = new YTDocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    generateCME(cmeDocument.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (YTConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (YTIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromEmbeddedToSBTreeWithCME() throws Exception {
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<YTIdentifiable> docsToAdd = new ArrayList<YTIdentifiable>();

    db.begin();
    YTDocument document = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      YTDocument docToAdd = new YTDocument();
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
      YTDocument docToAdd = new YTDocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());
    try {
      generateCME(rid);
      document.save(db.getClusterNameById(db.getDefaultClusterId()));
      db.commit();
      Assert.fail();
    } catch (YTConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (YTIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  private void generateCME(YTRID rid) throws InterruptedException {
    var th =
        new Thread(
            () -> {
              try (var session = db.copy()) {
                session.activateOnCurrentThread();
                session.begin();
                YTDocument cmeDocument = session.load(rid);

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
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    List<YTIdentifiable> docsToAdd = new ArrayList<YTIdentifiable>();

    db.begin();
    YTDocument document = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 10; i++) {
      YTDocument docToAdd = new YTDocument();
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
      YTIdentifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertFalse(ridBag.isEmbedded());

    for (YTIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromSBTreeToEmbeddedTxWithCME() throws Exception {
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    db.begin();
    YTDocument cmeDoc = new YTDocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    List<YTIdentifiable> docsToAdd = new ArrayList<YTIdentifiable>();

    YTDocument document = new YTDocument();

    ORidBag ridBag = new ORidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 10; i++) {
      YTDocument docToAdd = new YTDocument();
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
      YTIdentifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (YTConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertFalse(ridBag.isEmbedded());

    for (YTIdentifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  private void createDocsForLevel(
      final List<Integer> amountOfAddedDocsPerLevel,
      int level,
      int levels,
      Map<LevelKey, List<YTIdentifiable>> addedDocPerLevel,
      YTDocument rootDoc) {

    int docs = amountOfAddedDocsPerLevel.get(level);

    List<YTIdentifiable> addedDocs = new ArrayList<YTIdentifiable>();
    addedDocPerLevel.put(new LevelKey(rootDoc.getIdentity(), level), addedDocs);

    ORidBag ridBag = new ORidBag(db);
    rootDoc.field("ridBag", ridBag);

    for (int i = 0; i < docs; i++) {
      YTDocument docToAdd = new YTDocument();
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
      YTDatabaseSessionInternal db,
      List<Integer> amountOfDeletedDocsPerLevel,
      int level,
      int levels,
      YTDocument rootDoc,
      Random rnd) {
    rootDoc = db.bindToSession(rootDoc);
    ORidBag ridBag = rootDoc.field("ridBag");
    Iterator<YTIdentifiable> iter = ridBag.iterator();
    while (iter.hasNext()) {
      YTIdentifiable identifiable = iter.next();
      YTDocument doc = identifiable.getRecord();
      if (level + 1 < levels) {
        deleteDocsForLevel(db, amountOfDeletedDocsPerLevel, level + 1, levels, doc, rnd);
      }
    }

    int docs = amountOfDeletedDocsPerLevel.get(level);

    int k = 0;
    Iterator<YTIdentifiable> iterator = ridBag.iterator();
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
      YTDatabaseSessionInternal db,
      List<Integer> amountOfAddedDocsAfterSavePerLevel,
      int level,
      int levels,
      YTDocument rootDoc) {
    rootDoc = db.bindToSession(rootDoc);
    ORidBag ridBag = rootDoc.field("ridBag");

    for (YTIdentifiable identifiable : ridBag) {
      YTDocument doc = identifiable.getRecord();
      if (level + 1 < levels) {
        addDocsForLevel(db, amountOfAddedDocsAfterSavePerLevel, level + 1, levels, doc);
      }
    }

    int docs = amountOfAddedDocsAfterSavePerLevel.get(level);
    for (int i = 0; i < docs; i++) {
      YTDocument docToAdd = new YTDocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));

      ridBag.add(docToAdd);
    }
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void assertDocsAfterRollback(
      int level,
      int levels,
      Map<LevelKey, List<YTIdentifiable>> addedDocPerLevel,
      YTDocument rootDoc) {
    ORidBag ridBag = rootDoc.field("ridBag");
    List<YTIdentifiable> addedDocs =
        new ArrayList<YTIdentifiable>(
            addedDocPerLevel.get(new LevelKey(rootDoc.getIdentity(), level)));

    Iterator<YTIdentifiable> iterator = ridBag.iterator();
    while (iterator.hasNext()) {
      YTDocument doc = iterator.next().getRecord();
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

    private final YTRID rid;
    private final int level;

    private LevelKey(YTRID rid, int level) {
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
