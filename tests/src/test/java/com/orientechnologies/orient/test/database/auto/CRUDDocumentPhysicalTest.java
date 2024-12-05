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

import com.orientechnologies.core.db.document.YTDatabaseSessionAbstract;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.YTRecordAbstract;
import com.orientechnologies.core.record.impl.ODocumentInternal;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;


@Test
public class CRUDDocumentPhysicalTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public CRUDDocumentPhysicalTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    createProfileClass();
    createCompanyClass();
  }

  @Test
  public void create() {
    database.begin();
    database.command("delete from Account").close();
    database.commit();

    Assert.assertEquals(database.countClass("Account"), 0);

    fillInAccountData();

    database.begin();
    database.command("delete from Profile").close();
    database.commit();

    Assert.assertEquals(database.countClass("Profile"), 0);

    generateCompanyData();
  }

  @Test(dependsOnMethods = "create")
  public void testCreate() {
    Assert.assertEquals(database.countClass("Account", false), TOT_RECORDS_ACCOUNT);
  }

  @Test(dependsOnMethods = "testCreate")
  public void readAndBrowseDescendingAndCheckHoleUtilization() {
    // BROWSE IN THE OPPOSITE ORDER
    byte[] binary;

    Set<Integer> ids = new HashSet<>();
    for (int i = 0; i < TOT_RECORDS_ACCOUNT; i++) {
      ids.add(i);
    }

    var it = database.browseClass("Account", false);
    for (it.last(); it.hasPrevious(); ) {
      YTEntityImpl rec = it.previous();

      if (rec != null) {
        int id = ((Number) rec.field("id")).intValue();
        Assert.assertTrue(ids.remove(id));
        Assert.assertEquals(rec.field("name"), "Gipsy");
        Assert.assertEquals(rec.field("location"), "Italy");
        Assert.assertEquals(((Number) rec.field("testLong")).longValue(), 10000000000L);
        Assert.assertEquals(((Number) rec.field("salary")).intValue(), id + 300);
        Assert.assertNotNull(rec.field("extra"));
        Assert.assertEquals(((Byte) rec.field("value", Byte.class)).byteValue(), (byte) 10);

        binary = rec.field("binary", YTType.BINARY);

        for (int b = 0; b < binary.length; ++b) {
          Assert.assertEquals(binary[b], (byte) b);
        }
      }
    }

    Assert.assertTrue(ids.isEmpty());
  }

  @Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
  public void update() {
    int[] i = new int[1];

    var iterator = (Iterator<YTEntityImpl>) database.<YTEntityImpl>browseCluster("Account");
    database.forEachInTx(iterator, (session, rec) -> {
      if (i[0] % 2 == 0) {
        rec.field("location", "Spain");
      }

      rec.field("price", i[0] + 100);

      rec.save();

      i[0]++;
    });
  }

  @Test(dependsOnMethods = "update")
  public void testUpdate() {
    for (YTEntityImpl rec : database.<YTEntityImpl>browseCluster("Account")) {
      int price = ((Number) rec.field("price")).intValue();
      Assert.assertTrue(price - 100 >= 0);

      if ((price - 100) % 2 == 0) {
        Assert.assertEquals(rec.field("location"), "Spain");
      } else {
        Assert.assertEquals(rec.field("location"), "Italy");
      }
    }
  }

  @Test(dependsOnMethods = "testUpdate")
  public void testDoubleChanges() {
    checkEmbeddedDB();

    final Set<Integer> profileClusterIds =
        Arrays.stream(database.getMetadata().getSchema().getClass("Profile").getClusterIds())
            .asLongStream()
            .mapToObj(i -> (int) i)
            .collect(HashSet::new, HashSet::add, HashSet::addAll);

    database.begin();
    YTEntityImpl vDoc = database.newInstance();
    vDoc.setClassName("Profile");
    vDoc.field("nick", "JayM1").field("name", "Jay").field("surname", "Miner");
    vDoc.save();

    Assert.assertTrue(profileClusterIds.contains(vDoc.getIdentity().getClusterId()));

    vDoc = database.load(vDoc.getIdentity());
    vDoc.field("nick", "JayM2");
    vDoc.field("nick", "JayM3");
    vDoc.save();
    database.commit();

    @SuppressWarnings("deprecation")
    Set<OIndex> indexes =
        database.getMetadata().getSchema().getClass("Profile").getProperty("nick")
            .getIndexes(database);

    Assert.assertEquals(indexes.size(), 1);

    OIndex indexDefinition = indexes.iterator().next();
    try (final Stream<YTRID> stream = indexDefinition.getInternal().getRids(database, "JayM1")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    try (final Stream<YTRID> stream = indexDefinition.getInternal().getRids(database, "JayM2")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    try (Stream<YTRID> stream = indexDefinition.getInternal().getRids(database, "JayM3")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  @Test(dependsOnMethods = "testDoubleChanges")
  public void testMultiValues() {
    database.begin();
    YTEntityImpl vDoc = database.newInstance();
    vDoc.setClassName("Profile");
    vDoc.field("nick", "Jacky").field("name", "Jack").field("surname", "Tramiel");
    vDoc.save();

    // add a new record with the same name "nameA".
    vDoc = database.newInstance();
    vDoc.setClassName("Profile");
    vDoc.field("nick", "Jack").field("name", "Jack").field("surname", "Bauer");
    vDoc.save();
    database.commit();

    @SuppressWarnings("deprecation")
    Collection<OIndex> indexes =
        database.getMetadata().getSchema().getClass("Profile").getProperty("name")
            .getIndexes(database);
    Assert.assertEquals(indexes.size(), 1);

    OIndex indexName = indexes.iterator().next();
    // We must get 2 records for "nameA".
    try (Stream<YTRID> stream = indexName.getInternal().getRids(database, "Jack")) {
      Assert.assertEquals(stream.count(), 2);
    }

    database.begin();
    // Remove this last record.
    database.delete(database.bindToSession(vDoc));
    database.commit();

    // We must get 1 record for "nameA".
    try (Stream<YTRID> stream = indexName.getInternal().getRids(database, "Jack")) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test(dependsOnMethods = "testMultiValues")
  public void testUnderscoreField() {
    database.begin();
    YTEntityImpl vDoc = database.newInstance();
    vDoc.setClassName("Profile");
    vDoc.field("nick", "MostFamousJack")
        .field("name", "Kiefer")
        .field("surname", "Sutherland")
        .field("tag_list", new String[]{"actor", "myth"});
    vDoc.save();
    database.commit();

    @SuppressWarnings("deprecation")
    List<YTEntityImpl> result =
        database
            .command(
                new OSQLSynchQuery<YTEntityImpl>(
                    "select from Profile where name = 'Kiefer' and tag_list.size() > 0 "))
            .execute(database);

    Assert.assertEquals(result.size(), 1);
  }

  public void testLazyLoadingByLink() {
    database.begin();
    YTEntityImpl coreDoc = new YTEntityImpl();
    YTEntityImpl linkDoc = new YTEntityImpl();

    linkDoc.save(database.getClusterNameById(database.getDefaultClusterId()));
    coreDoc.field("link", linkDoc);
    coreDoc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    YTEntityImpl coreDocCopy = database.load(coreDoc.getIdentity());
    Assert.assertNotSame(coreDocCopy, coreDoc);

    coreDocCopy.setLazyLoad(false);
    Assert.assertTrue(coreDocCopy.field("link") instanceof YTRecordId);
    coreDocCopy.setLazyLoad(true);
    Assert.assertTrue(coreDocCopy.field("link") instanceof YTEntityImpl);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDbCacheUpdated() {
    database.createClassIfNotExist("Profile");
    database.begin();
    YTEntityImpl vDoc = database.newInstance();
    vDoc.setClassName("Profile");

    Set<String> tags = new HashSet<>();
    tags.add("test");
    tags.add("yeah");

    vDoc.field("nick", "Dexter")
        .field("name", "Michael")
        .field("surname", "Hall")
        .field("tag_list", tags);
    vDoc.save();
    database.commit();

    @SuppressWarnings("deprecation")
    List<YTEntityImpl> result =
        database
            .command(new OSQLSynchQuery<YTEntityImpl>("select from Profile where name = 'Michael'"))
            .execute(database);

    Assert.assertEquals(result.size(), 1);
    YTEntityImpl dexter = result.get(0);

    database.begin();
    dexter = database.bindToSession(dexter);
    ((Collection<String>) dexter.field("tag_list")).add("actor");

    dexter.setDirty();
    dexter.save();
    database.commit();

    //noinspection deprecation
    result =
        database
            .command(
                new OSQLSynchQuery<YTEntityImpl>(
                    "select from Profile where tag_list contains 'actor' and tag_list contains"
                        + " 'test'"))
            .execute(database);
    Assert.assertEquals(result.size(), 1);
  }

  @Test(dependsOnMethods = "testUnderscoreField")
  public void testUpdateLazyDirtyPropagation() {
    var iterator = (Iterator<YTEntityImpl>) database.<YTEntityImpl>browseCluster("Profile");
    database.forEachInTx(iterator, (session, rec) -> {
      Assert.assertFalse(rec.isDirty());
      Collection<?> followers = rec.field("followers");
      if (followers != null && !followers.isEmpty()) {
        followers.remove(followers.iterator().next());
        Assert.assertTrue(rec.isDirty());
        return false;
      }
      return true;
    });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNestedEmbeddedMap() {
    database.begin();
    YTEntityImpl newDoc = new YTEntityImpl();

    final Map<String, HashMap<?, ?>> map1 = new HashMap<>();
    newDoc.field("map1", map1, YTType.EMBEDDEDMAP);

    final Map<String, HashMap<?, ?>> map2 = new HashMap<>();
    map1.put("map2", (HashMap<?, ?>) map2);

    final Map<String, HashMap<?, ?>> map3 = new HashMap<>();
    map2.put("map3", (HashMap<?, ?>) map3);
    newDoc.save(database.getClusterNameById(database.getDefaultClusterId()));
    final YTRecordId rid = (YTRecordId) newDoc.getIdentity();
    database.commit();

    newDoc = database.bindToSession(newDoc);
    final YTEntityImpl loadedDoc = database.load(rid);
    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));

    Assert.assertTrue(loadedDoc.containsField("map1"));
    Assert.assertTrue(loadedDoc.field("map1") instanceof Map<?, ?>);
    final Map<String, YTEntityImpl> loadedMap1 = loadedDoc.field("map1");
    Assert.assertEquals(loadedMap1.size(), 1);

    Assert.assertTrue(loadedMap1.containsKey("map2"));
    Assert.assertTrue(loadedMap1.get("map2") instanceof Map<?, ?>);
    final Map<String, YTEntityImpl> loadedMap2 = (Map<String, YTEntityImpl>) loadedMap1.get("map2");
    Assert.assertEquals(loadedMap2.size(), 1);

    Assert.assertTrue(loadedMap2.containsKey("map3"));
    Assert.assertTrue(loadedMap2.get("map3") instanceof Map<?, ?>);
    final Map<String, YTEntityImpl> loadedMap3 = (Map<String, YTEntityImpl>) loadedMap2.get("map3");
    Assert.assertEquals(loadedMap3.size(), 0);
  }

  @Test
  public void commandWithPositionalParameters() {
    final OSQLSynchQuery<YTEntityImpl> query =
        new OSQLSynchQuery<>("select from Profile where name = ? and surname = ?");
    @SuppressWarnings("deprecation")
    List<YTEntityImpl> result = database.command(query).execute(database, "Barack", "Obama");

    Assert.assertFalse(result.isEmpty());
  }

  @Test(dependsOnMethods = "testCreate")
  public void queryWithPositionalParameters() {
    addBarackObamaAndFollowers();

    final OSQLSynchQuery<YTEntityImpl> query =
        new OSQLSynchQuery<>("select from Profile where name = ? and surname = ?");
    @SuppressWarnings("deprecation")
    List<YTEntityImpl> result = database.query(query, "Barack", "Obama");

    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void commandWithNamedParameters() {
    final OSQLSynchQuery<YTEntityImpl> query =
        new OSQLSynchQuery<>("select from Profile where name = :name and surname = :surname");

    HashMap<String, String> params = new HashMap<>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    addBarackObamaAndFollowers();

    @SuppressWarnings("deprecation")
    List<YTEntityImpl> result = database.command(query).execute(database, params);
    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void commandWrongParameterNames() {
    YTEntityImpl doc = database.newInstance();
    database.executeInTx(
        () -> {
          try {
            doc.field("a:b", 10);
            Assert.fail();
          } catch (IllegalArgumentException e) {
            Assert.assertTrue(true);
          }
        });

    database.executeInTx(
        () -> {
          try {
            doc.field("a,b", 10);
            Assert.fail();
          } catch (IllegalArgumentException e) {
            Assert.assertTrue(true);
          }
        });
  }

  @Test
  public void queryWithNamedParameters() {
    addBarackObamaAndFollowers();

    final OSQLSynchQuery<YTEntityImpl> query =
        new OSQLSynchQuery<>("select from Profile where name = :name and surname = :surname");

    HashMap<String, String> params = new HashMap<>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    @SuppressWarnings("deprecation")
    List<YTEntityImpl> result = database.query(query, params);

    Assert.assertFalse(result.isEmpty());
  }

  public void testJSONLinkd() {
    database.createClassIfNotExist("PersonTest");
    database.begin();
    YTEntityImpl jaimeDoc = new YTEntityImpl("PersonTest");
    jaimeDoc.field("name", "jaime");
    jaimeDoc.save();

    YTEntityImpl cerseiDoc = new YTEntityImpl("PersonTest");
    cerseiDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"cersei\",\"valonqar\":" + jaimeDoc.toJSON() + "}");
    cerseiDoc.save();
    database.commit();

    database.begin();
    jaimeDoc = database.bindToSession(jaimeDoc);
    // The link between jamie and tyrion is not saved properly
    YTEntityImpl tyrionDoc = new YTEntityImpl("PersonTest");
    tyrionDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.toJSON()
            + "}}");
    tyrionDoc.save();
    database.commit();

    for (YTEntityImpl o : database.browseClass("PersonTest")) {
      for (YTIdentifiable id : database.query("traverse * from " + o.getIdentity().toString())
          .stream().map(
              r -> r.getIdentity().orElseThrow()).toList()) {
        database.load(id.getIdentity()).toJSON();
      }
    }
  }

  @Test
  public void testDirtyChild() {
    YTEntityImpl parent = new YTEntityImpl();

    YTEntityImpl child1 = new YTEntityImpl();
    ODocumentInternal.addOwner(child1, parent);
    parent.field("child1", child1);

    Assert.assertTrue(child1.hasOwners());

    YTEntityImpl child2 = new YTEntityImpl();
    ODocumentInternal.addOwner(child2, child1);
    child1.field("child2", child2);

    Assert.assertTrue(child2.hasOwners());

    // BEFORE FIRST TOSTREAM
    Assert.assertTrue(parent.isDirty());
    parent.toStream();
    // AFTER TOSTREAM
    Assert.assertTrue(parent.isDirty());
    // CHANGE FIELDS VALUE (Automaticaly set dirty this child)
    child1.field("child2", new YTEntityImpl());
    Assert.assertTrue(parent.isDirty());
  }

  public void testEncoding() {
    String s = " \r\n\t:;,.|+*/\\=!?[]()'\"";

    database.begin();
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", s);
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    doc = database.bindToSession(doc);
    Assert.assertEquals(doc.field("test"), s);
  }

  @Test(dependsOnMethods = "create")
  public void polymorphicQuery() {
    database.begin();
    final YTRecordAbstract newAccount =
        new YTEntityImpl("Account").field("name", "testInheritanceName");
    newAccount.save();
    database.commit();

    List<YTEntityImpl> superClassResult = executeQuery("select from Account");

    List<YTEntityImpl> subClassResult = executeQuery("select from Company");

    Assert.assertFalse(superClassResult.isEmpty());
    Assert.assertFalse(subClassResult.isEmpty());
    Assert.assertTrue(superClassResult.size() >= subClassResult.size());

    // VERIFY ALL THE SUBCLASS RESULT ARE ALSO CONTAINED IN SUPERCLASS
    // RESULT
    for (YTEntityImpl d : subClassResult) {
      Assert.assertTrue(superClassResult.contains(d));
    }

    HashSet<YTEntityImpl> browsed = new HashSet<>();
    for (YTEntityImpl d : database.browseClass("Account")) {
      Assert.assertFalse(browsed.contains(d));
      browsed.add(d);
    }

    database.begin();
    database.bindToSession(newAccount).delete();
    database.commit();
  }

  @Test(dependsOnMethods = "testCreate")
  public void testBrowseClassHasNextTwice() {
    YTEntityImpl doc1 = null;
    //noinspection LoopStatementThatDoesntLoop
    for (Iterator<YTEntityImpl> itDoc = database.browseClass("Account"); itDoc.hasNext(); ) {
      doc1 = itDoc.next();
      break;
    }

    YTEntityImpl doc2 = null;
    //noinspection LoopStatementThatDoesntLoop
    for (Iterator<YTEntityImpl> itDoc = database.browseClass("Account"); itDoc.hasNext(); ) {
      //noinspection ResultOfMethodCallIgnored
      itDoc.hasNext();
      doc2 = itDoc.next();
      break;
    }

    Assert.assertEquals(doc1, doc2);
  }

  @Test(dependsOnMethods = "testCreate")
  public void nonPolymorphicQuery() {
    database.begin();
    final YTRecordAbstract newAccount =
        new YTEntityImpl("Account").field("name", "testInheritanceName");
    newAccount.save();
    database.commit();

    List<YTEntityImpl> allResult = executeQuery("select from Account");
    List<YTEntityImpl> superClassResult = executeQuery(
        "select from Account where @class = 'Account'");
    List<YTEntityImpl> subClassResult = executeQuery(
        "select from Company where @class = 'Company'");

    Assert.assertFalse(allResult.isEmpty());
    Assert.assertFalse(superClassResult.isEmpty());
    Assert.assertFalse(subClassResult.isEmpty());

    // VERIFY ALL THE SUBCLASS RESULT ARE NOT CONTAINED IN SUPERCLASS RESULT
    for (YTEntityImpl d : subClassResult) {
      Assert.assertFalse(superClassResult.contains(d));
    }

    HashSet<YTEntityImpl> browsed = new HashSet<>();
    for (YTEntityImpl d : database.browseClass("Account")) {
      Assert.assertFalse(browsed.contains(d));
      browsed.add(d);
    }

    database.begin();
    database.bindToSession(newAccount).delete();
    database.commit();
  }

  @Test(dependsOnMethods = "testCreate")
  public void testEmbeddeDocumentInTx() {
    YTEntityImpl bank = database.newInstance("Account");
    database.begin();

    bank.field("Name", "MyBank");

    YTEntityImpl bank2 = database.newInstance("Account");
    bank.field("embedded", bank2, YTType.EMBEDDED);
    bank.save();

    database.commit();

    database.close();

    database = acquireSession();

    database.begin();
    bank = database.bindToSession(bank);
    Assert.assertTrue(((YTEntityImpl) bank.field("embedded")).isEmbedded());
    Assert.assertFalse(((YTEntityImpl) bank.field("embedded")).getIdentity().isPersistent());
    database.rollback();

    database.begin();
    database.bindToSession(bank).delete();
    database.commit();
  }

  @Test(dependsOnMethods = "testCreate")
  public void testUpdateInChain() {
    database.begin();
    YTEntityImpl bank = database.newInstance("Account");
    bank.field("name", "MyBankChained");

    // EMBEDDED
    YTEntityImpl embedded = database.<YTEntityImpl>newInstance("Account")
        .field("name", "embedded1");
    bank.field("embedded", embedded, YTType.EMBEDDED);

    YTEntityImpl[] embeddeds =
        new YTEntityImpl[]{
            database.<YTEntityImpl>newInstance("Account").field("name", "embedded2"),
            database.<YTEntityImpl>newInstance("Account").field("name", "embedded3")
        };
    bank.field("embeddeds", embeddeds, YTType.EMBEDDEDLIST);

    // LINKED
    YTEntityImpl linked = database.<YTEntityImpl>newInstance("Account").field("name", "linked1");
    bank.field("linked", linked);

    YTEntityImpl[] linkeds =
        new YTEntityImpl[]{
            database.<YTEntityImpl>newInstance("Account").field("name", "linked2"),
            database.<YTEntityImpl>newInstance("Account").field("name", "linked3")
        };
    bank.field("linkeds", linkeds, YTType.LINKLIST);

    bank.save();
    database.commit();

    database.close();
    database = acquireSession();

    database.begin();
    bank = database.bindToSession(bank);
    YTEntityImpl changedDoc1 = bank.field("embedded.total", 100);
    // MUST CHANGE THE PARENT DOC BECAUSE IT'S EMBEDDED
    Assert.assertEquals(changedDoc1.field("name"), "MyBankChained");
    Assert.assertEquals(changedDoc1.<Object>field("embedded.total"), 100);

    YTEntityImpl changedDoc2 = bank.field("embeddeds.total", 200);
    // MUST CHANGE THE PARENT DOC BECAUSE IT'S EMBEDDED
    Assert.assertEquals(changedDoc2.field("name"), "MyBankChained");

    Collection<Integer> intEmbeddeds = changedDoc2.field("embeddeds.total");
    for (Integer e : intEmbeddeds) {
      Assert.assertEquals(e.intValue(), 200);
    }

    YTEntityImpl changedDoc3 = bank.field("linked.total", 300);
    // MUST CHANGE THE LINKED DOCUMENT
    Assert.assertEquals(changedDoc3.field("name"), "linked1");
    Assert.assertEquals(changedDoc3.<Object>field("total"), 300);
    database.commit();

    database.begin();
    bank = database.bindToSession(bank);
    ((YTEntityImpl) bank.field("linked")).delete();
    //noinspection unchecked
    for (YTIdentifiable l : (Collection<YTIdentifiable>) bank.field("linkeds")) {
      l.getRecord().delete();
    }
    bank.delete();
    database.commit();
  }

  public void testSerialization() {
    ORecordSerializer current = YTDatabaseSessionAbstract.getDefaultSerializer();
    YTDatabaseSessionAbstract.setDefaultSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
    ORecordSerializer dbser = database.getSerializer();
    database.setSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
    final byte[] streamOrigin =
        "Account@html:{\"path\":\"html/layout\"},config:{\"title\":\"Github Admin\",\"modules\":(githubDisplay:\"github_display\")},complex:(simple1:\"string1\",one_level1:(simple2:\"string2\"),two_levels:(simple3:\"string3\",one_level2:(simple4:\"string4\")))"
            .getBytes();
    YTEntityImpl doc =
        (YTEntityImpl)
            ORecordSerializerSchemaAware2CSV.INSTANCE.fromStream(database,
                streamOrigin, new YTEntityImpl(), null);
    doc.field("out");
    final byte[] streamDest = ORecordSerializerSchemaAware2CSV.INSTANCE.toStream(database, doc);
    Assert.assertEquals(streamOrigin, streamDest);
    YTDatabaseSessionAbstract.setDefaultSerializer(current);
    database.setSerializer(dbser);
  }

  @Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
  public void testUpdateNoVersionCheck() {
    List<YTEntityImpl> result = executeQuery("select from Account");

    database.begin();
    YTEntityImpl doc = database.bindToSession(result.get(0));
    doc.field("name", "modified");
    int oldVersion = doc.getVersion();

    ORecordInternal.setVersion(doc, -2);

    doc.save();
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    Assert.assertEquals(doc.getVersion(), oldVersion);
    Assert.assertEquals(doc.field("name"), "modified");
    database.commit();
  }

  public void testCreateEmbddedClassDocument() {
    final YTSchema schema = database.getMetadata().getSchema();
    final String SUFFIX = "TESTCLUSTER1";

    YTClass testClass1 = schema.createClass("testCreateEmbddedClass1");
    YTClass testClass2 = schema.createClass("testCreateEmbddedClass2");
    testClass2.createProperty(database, "testClass1Property", YTType.EMBEDDED, testClass1);

    int clusterId = database.addCluster("testCreateEmbddedClass2" + SUFFIX);
    schema.getClass("testCreateEmbddedClass2").addClusterId(database, clusterId);

    testClass1 = schema.getClass("testCreateEmbddedClass1");
    testClass2 = schema.getClass("testCreateEmbddedClass2");

    database.begin();
    YTEntityImpl testClass2Document = new YTEntityImpl(testClass2);
    testClass2Document.field("testClass1Property", new YTEntityImpl(testClass1));
    testClass2Document.save("testCreateEmbddedClass2" + SUFFIX);
    database.commit();

    testClass2Document = database.load(testClass2Document.getIdentity());
    Assert.assertNotNull(testClass2Document);

    Assert.assertEquals(testClass2Document.getSchemaClass(), testClass2);

    YTEntityImpl embeddedDoc = testClass2Document.field("testClass1Property");
    Assert.assertEquals(embeddedDoc.getSchemaClass(), testClass1);
  }

  public void testRemoveAllLinkList() {
    YTEntityImpl doc = new YTEntityImpl();

    final List<YTEntityImpl> allDocs = new ArrayList<>();

    database.begin();
    for (int i = 0; i < 10; i++) {
      final YTEntityImpl linkDoc = new YTEntityImpl();
      linkDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

      allDocs.add(linkDoc);
    }
    doc.field("linkList", allDocs);
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    final List<YTEntityImpl> docsToRemove = new ArrayList<>(allDocs.size() / 2);
    for (int i = 0; i < 5; i++) {
      docsToRemove.add(allDocs.get(i));
    }

    doc = database.bindToSession(doc);
    List<YTIdentifiable> linkList = doc.field("linkList");
    linkList.removeAll(docsToRemove);

    Assert.assertEquals(linkList.size(), 5);

    for (int i = 5; i < 10; i++) {
      Assert.assertEquals(linkList.get(i - 5), allDocs.get(i));
    }
    doc.save();
    database.commit();

    database.begin();
    database.bindToSession(doc).save();
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    linkList = doc.field("linkList");
    Assert.assertEquals(linkList.size(), 5);

    for (int i = 5; i < 10; i++) {
      Assert.assertEquals(linkList.get(i - 5), allDocs.get(i));
    }
    database.commit();
  }

  public void testRemoveAndReload() {
    YTEntityImpl doc1;

    database.begin();
    {
      doc1 = new YTEntityImpl();
      doc1.save(database.getClusterNameById(database.getDefaultClusterId()));
    }
    database.commit();

    database.begin();
    doc1 = database.bindToSession(doc1);
    database.delete(doc1);
    database.commit();

    database.begin();
    try {
      database.load(doc1.getIdentity());
      Assert.fail();
    } catch (YTRecordNotFoundException rnf) {
      // ignore
    }

    database.commit();
  }

  @Test
  public void testAny() {
    database.command("create class TestExport").close();
    database.command("create property TestExport.anything ANY").close();

    database.begin();
    database.command("insert into TestExport set anything = 3").close();
    database.command("insert into TestExport set anything = 'Jay'").close();
    database.command("insert into TestExport set anything = 2.3").close();
    database.commit();

    YTResultSet result = database.command("select count(*) from TestExport where anything = 3");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stream().count(), 1);

    result = database.command("select count(*) from TestExport where anything = 'Jay'");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stream().count(), 1);

    result = database.command("select count(*) from TestExport where anything = 2.3");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stream().count(), 1);
  }
}
