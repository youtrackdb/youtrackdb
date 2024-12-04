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

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.YTSerializationException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
@Test
public class JSONTest extends DocumentDBBaseTest {

  public static final String FORMAT_WITHOUT_TYPES =
      "rid,version,class,type,attribSameRow,alwaysFetchEmbedded,fetchPlan:*:0";

  @Parameters(value = "remote")
  public JSONTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    addBarackObamaAndFollowers();
  }

  @Test
  public void testAlmostLink() {
    final YTDocument doc = new YTDocument();
    doc.fromJSON("{'title': '#330: Dollar Coins Are Done'}");
  }

  @Test
  public void testNullList() {
    final YTDocument documentSource = new YTDocument();
    documentSource.fromJSON("{\"list\" : [\"string\", null]}");

    final YTDocument documentTarget = new YTDocument();
    ORecordInternal.unsetDirty(documentTarget);
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", YTType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), "string");
    Assert.assertNull(list.get(1));
  }

  @Test
  public void testBooleanList() {
    final YTDocument documentSource = new YTDocument();
    documentSource.fromJSON("{\"list\" : [true, false]}");

    final YTDocument documentTarget = new YTDocument();
    ORecordInternal.unsetDirty(documentTarget);
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", YTType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), true);
    Assert.assertEquals(list.get(1), false);
  }

  @Test
  public void testNumericIntegerList() {
    final YTDocument documentSource = new YTDocument();
    documentSource.fromJSON("{\"list\" : [17,42]}");

    final YTDocument documentTarget = new YTDocument();
    ORecordInternal.unsetDirty(documentTarget);
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", YTType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), 17);
    Assert.assertEquals(list.get(1), 42);
  }

  @Test
  public void testNumericLongList() {
    final YTDocument documentSource = new YTDocument();
    documentSource.fromJSON("{\"list\" : [100000000000,100000000001]}");

    final YTDocument documentTarget = new YTDocument();
    ORecordInternal.unsetDirty(documentTarget);
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", YTType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), 100000000000L);
    Assert.assertEquals(list.get(1), 100000000001L);
  }

  @Test
  public void testNumericFloatList() {
    final YTDocument documentSource = new YTDocument();
    documentSource.fromJSON("{\"list\" : [17.3,42.7]}");

    final YTDocument documentTarget = new YTDocument();
    ORecordInternal.unsetDirty(documentTarget);
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", YTType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), 17.3);
    Assert.assertEquals(list.get(1), 42.7);
  }

  @Test
  public void testNullity() {
    final YTDocument doc = new YTDocument();
    doc.fromJSON(
        "{\"gender\":{\"name\":\"Male\"},\"firstName\":\"Jack\",\"lastName\":\"Williams\",\"phone\":\"561-401-3348\",\"email\":\"0586548571@example.com\",\"address\":{\"street1\":\"Smith"
            + " Ave\","
            + "\"street2\":null,\"city\":\"GORDONSVILLE\",\"state\":\"VA\",\"code\":\"22942\"},\"dob\":\"2011-11-17"
            + " 03:17:04\"}");
    final String json = doc.toJSON();
    final YTDocument loadedDoc = new YTDocument();
    loadedDoc.fromJSON(json);
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
  }

  @Test
  public void testNanNoTypes() {
    YTDocument doc = new YTDocument();
    String input =
        "{\"@type\":\"d\",\"@version\":0,\"nan\":null,\"p_infinity\":null,\"n_infinity\":null}";
    doc.field("nan", Double.NaN);
    doc.field("p_infinity", Double.POSITIVE_INFINITY);
    doc.field("n_infinity", Double.NEGATIVE_INFINITY);
    String json = doc.toJSON(FORMAT_WITHOUT_TYPES);
    Assert.assertEquals(json, input);

    doc = new YTDocument();
    input = "{\"@type\":\"d\",\"@version\":0,\"nan\":null,\"p_infinity\":null,\"n_infinity\":null}";
    doc.field("nan", Float.NaN);
    doc.field("p_infinity", Float.POSITIVE_INFINITY);
    doc.field("n_infinity", Float.NEGATIVE_INFINITY);
    json = doc.toJSON(FORMAT_WITHOUT_TYPES);
    Assert.assertEquals(json, input);
  }

  @Test
  public void testEmbeddedList() {
    final YTDocument doc = new YTDocument();
    final List<YTDocument> list = new ArrayList<YTDocument>();
    doc.field("embeddedList", list, YTType.EMBEDDEDLIST);
    list.add(new YTDocument().field("name", "Luca"));
    list.add(new YTDocument().field("name", "Marcus"));

    final String json = doc.toJSON();
    final YTDocument loadedDoc = new YTDocument();
    loadedDoc.fromJSON(json);
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
    Assert.assertTrue(loadedDoc.containsField("embeddedList"));
    Assert.assertTrue(loadedDoc.field("embeddedList") instanceof List<?>);
    Assert.assertTrue(
        ((List<YTDocument>) loadedDoc.field("embeddedList")).get(0) instanceof YTDocument);

    YTDocument newDoc = ((List<YTDocument>) loadedDoc.field("embeddedList")).get(0);
    Assert.assertEquals(newDoc.field("name"), "Luca");
    newDoc = ((List<YTDocument>) loadedDoc.field("embeddedList")).get(1);
    Assert.assertEquals(newDoc.field("name"), "Marcus");
  }

  @Test
  public void testEmbeddedMap() {
    final YTDocument doc = new YTDocument();

    final Map<String, YTDocument> map = new HashMap<String, YTDocument>();
    doc.field("map", map);
    map.put("Luca", new YTDocument().field("name", "Luca"));
    map.put("Marcus", new YTDocument().field("name", "Marcus"));
    map.put("Cesare", new YTDocument().field("name", "Cesare"));

    final String json = doc.toJSON();
    final YTDocument loadedDoc = new YTDocument();
    loadedDoc.fromJSON(json);

    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));

    Assert.assertTrue(loadedDoc.containsField("map"));
    Assert.assertTrue(loadedDoc.field("map") instanceof Map<?, ?>);
    Assert.assertTrue(
        ((Map<String, YTDocument>) loadedDoc.field("map")).values().iterator().next()
            instanceof YTDocument);

    YTDocument newDoc = ((Map<String, YTDocument>) loadedDoc.field("map")).get("Luca");
    Assert.assertEquals(newDoc.field("name"), "Luca");

    newDoc = ((Map<String, YTDocument>) loadedDoc.field("map")).get("Marcus");
    Assert.assertEquals(newDoc.field("name"), "Marcus");

    newDoc = ((Map<String, YTDocument>) loadedDoc.field("map")).get("Cesare");
    Assert.assertEquals(newDoc.field("name"), "Cesare");
  }

  @Test
  public void testListToJSON() {
    final List<YTDocument> list = new ArrayList<YTDocument>();
    final YTDocument first = new YTDocument().field("name", "Luca");
    final YTDocument second = new YTDocument().field("name", "Marcus");
    list.add(first);
    list.add(second);

    final String jsonResult = OJSONWriter.listToJSON(list, null);
    final YTDocument doc = new YTDocument();
    doc.fromJSON("{\"result\": " + jsonResult + "}");
    Collection<YTDocument> result = doc.field("result");
    Assert.assertTrue(result instanceof Collection);
    Assert.assertEquals(result.size(), 2);
    for (final YTDocument resultDoc : result) {
      Assert.assertTrue(first.hasSameContentOf(resultDoc) || second.hasSameContentOf(resultDoc));
    }
  }

  @Test
  public void testEmptyEmbeddedMap() {
    final YTDocument doc = new YTDocument();

    final Map<String, YTDocument> map = new HashMap<String, YTDocument>();
    doc.field("embeddedMap", map, YTType.EMBEDDEDMAP);

    final String json = doc.toJSON();
    final YTDocument loadedDoc = new YTDocument();
    loadedDoc.fromJSON(json);
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);

    final Map<String, YTDocument> loadedMap = loadedDoc.field("embeddedMap");
    Assert.assertEquals(loadedMap.size(), 0);
  }

  @Test
  public void testMultiLevelTypes() {
    String oldDataTimeFormat = database.get(ATTRIBUTES.DATETIMEFORMAT).toString();
    database.set(
        ATTRIBUTES.DATETIMEFORMAT, OStorageConfiguration.DEFAULT_DATETIME_FORMAT);
    try {
      YTDocument newDoc = new YTDocument();
      newDoc.field("long", 100000000000L);
      newDoc.field("date", new Date());
      newDoc.field("byte", (byte) 12);
      YTDocument firstLevelDoc = new YTDocument();
      firstLevelDoc.field("long", 200000000000L);
      firstLevelDoc.field("date", new Date());
      firstLevelDoc.field("byte", (byte) 13);
      YTDocument secondLevelDoc = new YTDocument();
      secondLevelDoc.field("long", 300000000000L);
      secondLevelDoc.field("date", new Date());
      secondLevelDoc.field("byte", (byte) 14);
      YTDocument thirdLevelDoc = new YTDocument();
      thirdLevelDoc.field("long", 400000000000L);
      thirdLevelDoc.field("date", new Date());
      thirdLevelDoc.field("byte", (byte) 15);
      newDoc.field("doc", firstLevelDoc);
      firstLevelDoc.field("doc", secondLevelDoc);
      secondLevelDoc.field("doc", thirdLevelDoc);

      final String json = newDoc.toJSON();
      YTDocument loadedDoc = new YTDocument();
      loadedDoc.fromJSON(json);

      Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));
      Assert.assertTrue(loadedDoc.field("long") instanceof Long);
      Assert.assertEquals(
          ((Long) newDoc.field("long")).longValue(), ((Long) loadedDoc.field("long")).longValue());
      Assert.assertTrue(loadedDoc.field("date") instanceof Date);
      Assert.assertTrue(loadedDoc.field("byte") instanceof Byte);
      Assert.assertEquals(
          ((Byte) newDoc.field("byte")).byteValue(), ((Byte) loadedDoc.field("byte")).byteValue());
      Assert.assertTrue(loadedDoc.field("doc") instanceof YTDocument);

      YTDocument firstDoc = loadedDoc.field("doc");
      Assert.assertTrue(firstLevelDoc.hasSameContentOf(firstDoc));
      Assert.assertTrue(firstDoc.field("long") instanceof Long);
      Assert.assertEquals(
          ((Long) firstLevelDoc.field("long")).longValue(),
          ((Long) firstDoc.field("long")).longValue());
      Assert.assertTrue(firstDoc.field("date") instanceof Date);
      Assert.assertTrue(firstDoc.field("byte") instanceof Byte);
      Assert.assertEquals(
          ((Byte) firstLevelDoc.field("byte")).byteValue(),
          ((Byte) firstDoc.field("byte")).byteValue());
      Assert.assertTrue(firstDoc.field("doc") instanceof YTDocument);

      YTDocument secondDoc = firstDoc.field("doc");
      Assert.assertTrue(secondLevelDoc.hasSameContentOf(secondDoc));
      Assert.assertTrue(secondDoc.field("long") instanceof Long);
      Assert.assertEquals(
          ((Long) secondLevelDoc.field("long")).longValue(),
          ((Long) secondDoc.field("long")).longValue());
      Assert.assertTrue(secondDoc.field("date") instanceof Date);
      Assert.assertTrue(secondDoc.field("byte") instanceof Byte);
      Assert.assertEquals(
          ((Byte) secondLevelDoc.field("byte")).byteValue(),
          ((Byte) secondDoc.field("byte")).byteValue());
      Assert.assertTrue(secondDoc.field("doc") instanceof YTDocument);

      YTDocument thirdDoc = secondDoc.field("doc");
      Assert.assertTrue(thirdLevelDoc.hasSameContentOf(thirdDoc));
      Assert.assertTrue(thirdDoc.field("long") instanceof Long);
      Assert.assertEquals(
          ((Long) thirdLevelDoc.field("long")).longValue(),
          ((Long) thirdDoc.field("long")).longValue());
      Assert.assertTrue(thirdDoc.field("date") instanceof Date);
      Assert.assertTrue(thirdDoc.field("byte") instanceof Byte);
      Assert.assertEquals(
          ((Byte) thirdLevelDoc.field("byte")).byteValue(),
          ((Byte) thirdDoc.field("byte")).byteValue());
    } finally {
      database.set(ATTRIBUTES.DATETIMEFORMAT, oldDataTimeFormat);
    }
  }

  @Test
  public void testMerge() {
    YTDocument doc1 = new YTDocument();
    final ArrayList<String> list = new ArrayList<String>();
    doc1.field("embeddedList", list, YTType.EMBEDDEDLIST);
    list.add("Luca");
    list.add("Marcus");
    list.add("Jay");
    doc1.field("salary", 10000);
    doc1.field("years", 16);

    YTDocument doc2 = new YTDocument();
    final ArrayList<String> list2 = new ArrayList<String>();
    doc2.field("embeddedList", list2, YTType.EMBEDDEDLIST);
    list2.add("Luca");
    list2.add("Michael");
    doc2.field("years", 32);

    YTDocument docMerge1 = doc1.copy();
    docMerge1.merge(doc2, true, true);

    Assert.assertTrue(docMerge1.containsField("embeddedList"));
    Assert.assertTrue(docMerge1.field("embeddedList") instanceof List<?>);
    Assert.assertEquals(((List<String>) docMerge1.field("embeddedList")).size(), 4);
    Assert.assertTrue(((List<String>) docMerge1.field("embeddedList")).get(0) instanceof String);
    Assert.assertEquals(((Integer) docMerge1.field("salary")).intValue(), 10000);
    Assert.assertEquals(((Integer) docMerge1.field("years")).intValue(), 32);

    YTDocument docMerge2 = doc1.copy();
    docMerge2.merge(doc2, true, false);

    Assert.assertTrue(docMerge2.containsField("embeddedList"));
    Assert.assertTrue(docMerge2.field("embeddedList") instanceof List<?>);
    Assert.assertEquals(((List<String>) docMerge2.field("embeddedList")).size(), 2);
    Assert.assertTrue(((List<String>) docMerge2.field("embeddedList")).get(0) instanceof String);
    Assert.assertEquals(((Integer) docMerge2.field("salary")).intValue(), 10000);
    Assert.assertEquals(((Integer) docMerge2.field("years")).intValue(), 32);

    YTDocument docMerge3 = doc1.copy();

    doc2.removeField("years");
    docMerge3.merge(doc2, false, false);

    Assert.assertTrue(docMerge3.containsField("embeddedList"));
    Assert.assertTrue(docMerge3.field("embeddedList") instanceof List<?>);
    Assert.assertEquals(((List<String>) docMerge3.field("embeddedList")).size(), 2);
    Assert.assertTrue(((List<String>) docMerge3.field("embeddedList")).get(0) instanceof String);
    Assert.assertFalse(docMerge3.containsField("salary"));
    Assert.assertFalse(docMerge3.containsField("years"));
  }

  @Test
  public void testNestedEmbeddedMap() {
    YTDocument newDoc = new YTDocument();

    final Map<String, HashMap<?, ?>> map1 = new HashMap<String, HashMap<?, ?>>();
    newDoc.field("map1", map1, YTType.EMBEDDEDMAP);

    final Map<String, HashMap<?, ?>> map2 = new HashMap<String, HashMap<?, ?>>();
    map1.put("map2", (HashMap<?, ?>) map2);

    final Map<String, HashMap<?, ?>> map3 = new HashMap<String, HashMap<?, ?>>();
    map2.put("map3", (HashMap<?, ?>) map3);

    String json = newDoc.toJSON();
    YTDocument loadedDoc = new YTDocument();
    loadedDoc.fromJSON(json);

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));

    Assert.assertTrue(loadedDoc.containsField("map1"));
    Assert.assertTrue(loadedDoc.field("map1") instanceof Map<?, ?>);
    final Map<String, YTDocument> loadedMap1 = loadedDoc.field("map1");
    Assert.assertEquals(loadedMap1.size(), 1);

    Assert.assertTrue(loadedMap1.containsKey("map2"));
    Assert.assertTrue(loadedMap1.get("map2") instanceof Map<?, ?>);
    final Map<String, YTDocument> loadedMap2 = (Map<String, YTDocument>) loadedMap1.get("map2");
    Assert.assertEquals(loadedMap2.size(), 1);

    Assert.assertTrue(loadedMap2.containsKey("map3"));
    Assert.assertTrue(loadedMap2.get("map3") instanceof Map<?, ?>);
    final Map<String, YTDocument> loadedMap3 = (Map<String, YTDocument>) loadedMap2.get("map3");
    Assert.assertEquals(loadedMap3.size(), 0);
  }

  @Test
  public void testFetchedJson() {
    List<YTDocument> result =
        database
            .command("select * from Profile where name = 'Barack' and surname = 'Obama'")
            .stream()
            .map((e) -> (YTDocument) e.toElement())
            .toList();

    for (YTDocument doc : result) {
      String jsonFull =
          doc.toJSON("type,rid,version,class,keepTypes,attribSameRow,indent:0,fetchPlan:*:-1");
      YTDocument loadedDoc = new YTDocument();
      loadedDoc.fromJSON(jsonFull);

      Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
    }
  }

  // Requires JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES
  public void testSpecialChar() {
    YTDocument doc = new YTDocument();
    doc.fromJSON(
        "{name:{\"%Field\":[\"value1\",\"value2\"],\"%Field2\":{},\"%Field3\":\"value3\"}}");
    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);

    Map<String, ?> map = doc.toMap();
    final YTDocument docToCompare = new YTDocument(database);
    docToCompare.fromMap(map);

    final YTDocument loadedDoc = database.load(doc.getIdentity());

    Assert.assertTrue(
        docToCompare.hasSameContentOf(loadedDoc));
    database.commit();
  }

  public void testArrayOfArray() {
    YTDocument doc = new YTDocument();
    doc.fromJSON(
        "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ [ 100,"
            + "  0 ],  [ 101, 1 ] ]}");

    database.begin();
    doc.save();
    database.commit();
    database.begin();
    doc = database.bindToSession(doc);

    Map<String, ?> map = doc.toMap();
    final YTDocument docToCompare = new YTDocument(database);
    docToCompare.fromMap(map);

    final YTDocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertTrue(docToCompare.hasSameContentOf(loadedDoc));
  }

  public void testLongTypes() {
    YTDocument doc = new YTDocument();
    doc.fromJSON(
        "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ ["
            + " 32874387347347,  0 ],  [ -23736753287327, 1 ] ]}");

    database.begin();
    doc.save();
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    Map<String, ?> map = doc.toMap();
    final YTDocument docToCompare = new YTDocument(database);
    docToCompare.fromMap(map);

    final YTDocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertTrue(docToCompare.hasSameContentOf(loadedDoc));
    database.commit();
  }

  // Requires JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES
  public void testSpecialChars() {
    YTDocument doc = new YTDocument();
    doc.fromJSON(
        "{Field:{\"Key1\":[\"Value1\",\"Value2\"],\"Key2\":{\"%%dummy%%\":null},\"Key3\":\"Value3\"}}");
    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();
    database.begin();
    doc = database.bindToSession(doc);

    Map<String, ?> map = doc.toMap();
    final YTDocument docToCompare = new YTDocument(database);
    docToCompare.fromMap(map);

    final YTDocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertEquals(docToCompare, loadedDoc);

    database.commit();
  }

  public void testJsonToStream() {
    final String doc1Json =
        "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}";
    final YTDocument doc1 = new YTDocument();
    doc1.fromJSON(doc1Json);
    final String doc1String = new String(
        ORecordSerializerSchemaAware2CSV.INSTANCE.toStream(database, doc1));
    Assert.assertEquals(doc1Json, "{" + doc1String + "}");

    final String doc2Json =
        "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}";
    final YTDocument doc2 = new YTDocument();
    doc2.fromJSON(doc2Json);
    final String doc2String = new String(
        ORecordSerializerSchemaAware2CSV.INSTANCE.toStream(database, doc2));
    Assert.assertEquals(doc2Json, "{" + doc2String + "}");
  }

  public void testSameNameCollectionsAndMap() {
    YTDocument doc = new YTDocument();
    doc.field("string", "STRING_VALUE");
    List<YTDocument> list = new ArrayList<YTDocument>();
    for (int i = 0; i < 1; i++) {
      final YTDocument doc1 = new YTDocument();
      doc.field("number", i);
      list.add(doc1);
      Map<String, YTDocument> docMap = new HashMap<String, YTDocument>();
      for (int j = 0; j < 1; j++) {
        YTDocument doc2 = new YTDocument();
        doc2.field("blabla", j);
        docMap.put(String.valueOf(j), doc2);
        YTDocument doc3 = new YTDocument();
        doc3.field("blubli", String.valueOf(i + j));
        doc2.field("out", doc3);
      }
      doc1.field("out", docMap);
      list.add(doc1);
    }
    doc.field("out", list);

    String json = doc.toJSON();
    YTDocument newDoc = new YTDocument();
    newDoc.fromJSON(json);

    Assert.assertEquals(json, newDoc.toJSON());
    Assert.assertTrue(newDoc.hasSameContentOf(doc));

    doc = new YTDocument();
    doc.field("string", "STRING_VALUE");
    final Map<String, YTDocument> docMap = new HashMap<String, YTDocument>();
    for (int i = 0; i < 10; i++) {
      YTDocument doc1 = new YTDocument();
      doc.field("number", i);
      list.add(doc1);
      list = new ArrayList<>();
      for (int j = 0; j < 5; j++) {
        YTDocument doc2 = new YTDocument();
        doc2.field("blabla", j);
        list.add(doc2);
        YTDocument doc3 = new YTDocument();
        doc3.field("blubli", String.valueOf(i + j));
        doc2.field("out", doc3);
      }
      doc1.field("out", list);
      docMap.put(String.valueOf(i), doc1);
    }
    doc.field("out", docMap);
    json = doc.toJSON();
    newDoc = new YTDocument();
    newDoc.fromJSON(json);
    Assert.assertEquals(newDoc.toJSON(), json);
    Assert.assertTrue(newDoc.hasSameContentOf(doc));
  }

  public void testSameNameCollectionsAndMap2() {
    YTDocument doc = new YTDocument();
    doc.field("string", "STRING_VALUE");
    List<YTDocument> list = new ArrayList<YTDocument>();
    for (int i = 0; i < 2; i++) {
      YTDocument doc1 = new YTDocument();
      list.add(doc1);
      Map<String, YTDocument> docMap = new HashMap<String, YTDocument>();
      for (int j = 0; j < 5; j++) {
        YTDocument doc2 = new YTDocument();
        doc2.field("blabla", j);
        docMap.put(String.valueOf(j), doc2);
      }
      doc1.field("theMap", docMap);
      list.add(doc1);
    }
    doc.field("theList", list);
    String json = doc.toJSON();
    YTDocument newDoc = new YTDocument();
    newDoc.fromJSON(json);
    Assert.assertEquals(newDoc.toJSON(), json);
    Assert.assertTrue(newDoc.hasSameContentOf(doc));
  }

  public void testSameNameCollectionsAndMap3() {
    YTDocument doc = new YTDocument();
    doc.field("string", "STRING_VALUE");
    List<Map<String, YTDocument>> list = new ArrayList<Map<String, YTDocument>>();
    for (int i = 0; i < 2; i++) {
      Map<String, YTDocument> docMap = new HashMap<String, YTDocument>();
      for (int j = 0; j < 5; j++) {
        YTDocument doc1 = new YTDocument();
        doc1.field("blabla", j);
        docMap.put(String.valueOf(j), doc1);
      }

      list.add(docMap);
    }
    doc.field("theList", list);
    String json = doc.toJSON();
    YTDocument newDoc = new YTDocument();
    newDoc.fromJSON(json);
    Assert.assertEquals(newDoc.toJSON(), json);
  }

  public void testNestedJsonCollection() {
    if (!database.getMetadata().getSchema().existsClass("Device")) {
      database.getMetadata().getSchema().createClass("Device");
    }

    database.begin();
    database
        .command(
            "insert into device (resource_id, domainset) VALUES (0, [ { 'domain' : 'abc' }, {"
                + " 'domain' : 'pqr' } ])")
        .close();
    database.commit();

    OResultSet result = database.query("select from device where domainset.domain contains 'abc'");
    Assert.assertTrue(result.stream().count() > 0);

    result = database.query("select from device where domainset[domain = 'abc'] is not null");
    Assert.assertTrue(result.stream().count() > 0);

    result = database.query("select from device where domainset.domain contains 'pqr'");
    Assert.assertTrue(result.stream().count() > 0);
  }

  public void testNestedEmbeddedJson() {
    if (!database.getMetadata().getSchema().existsClass("Device")) {
      database.getMetadata().getSchema().createClass("Device");
    }

    database.begin();
    database
        .command("insert into device (resource_id, domainset) VALUES (1, { 'domain' : 'eee' })")
        .close();
    database.commit();

    OResultSet result = database.query("select from device where domainset.domain = 'eee'");
    Assert.assertTrue(result.stream().count() > 0);
  }

  public void testNestedMultiLevelEmbeddedJson() {
    if (!database.getMetadata().getSchema().existsClass("Device")) {
      database.getMetadata().getSchema().createClass("Device");
    }

    database.begin();
    database
        .command(
            "insert into device (domainset) values ({'domain' : { 'lvlone' : { 'value' : 'five' } }"
                + " } )")
        .close();
    database.commit();

    OResultSet result =
        database.query("select from device where domainset.domain.lvlone.value = 'five'");

    Assert.assertTrue(result.stream().count() > 0);
  }

  public void testSpaces() {
    YTDocument doc = new YTDocument();
    String test =
        "{"
            + "\"embedded\": {"
            + "\"second_embedded\":  {"
            + "\"text\":\"this is a test\""
            + "}"
            + "}"
            + "}";
    doc.fromJSON(test);
    Assert.assertTrue(doc.toJSON("fetchPlan:*:0,rid").indexOf("this is a test") > -1);
  }

  public void testEscaping() {
    YTDocument doc = new YTDocument();
    String s =
        "{\"name\": \"test\", \"nested\": { \"key\": \"value\", \"anotherKey\": 123 }, \"deep\":"
            + " {\"deeper\": { \"k\": \"v\",\"quotes\": \"\\\"\\\",\\\"oops\\\":\\\"123\\\"\","
            + " \"likeJson\": \"[1,2,3]\",\"spaces\": \"value with spaces\"}}}";
    doc.fromJSON(s);
    Assert.assertEquals(doc.field("deep[deeper][quotes]"), "\"\",\"oops\":\"123\"");

    String res = doc.toJSON();

    // LOOK FOR "quotes": \"\",\"oops\":\"123\"
    Assert.assertTrue(res.contains("\"quotes\":\"\\\"\\\",\\\"oops\\\":\\\"123\\\"\""));
  }

  public void testEscapingDoubleQuotes() {
    final YTDocument doc = new YTDocument();
    String sb =
        " {\n"
            + "    \"foo\":{\n"
            + "            \"bar\":{\n"
            + "                \"P357\":[\n"
            + "                            {\n"
            + "\n"
            + "                                \"datavalue\":{\n"
            + "                                    \"value\":\"\\\"\\\"\" \n"
            + "                                }\n"
            + "                        }\n"
            + "                ]   \n"
            + "            },\n"
            + "            \"three\": \"a\"\n"
            + "        }\n"
            + "} ";
    doc.fromJSON(sb);
    Assert.assertEquals(doc.field("foo.three"), "a");
    final Collection c = doc.field("foo.bar.P357");
    Assert.assertEquals(c.size(), 1);
    final Map doc2 = (Map) c.iterator().next();
    Assert.assertEquals(((Map) doc2.get("datavalue")).get("value"), "\"\"");
  }

  public void testEscapingDoubleQuotes2() {
    final YTDocument doc = new YTDocument();
    String sb =
        " {\n"
            + "    \"foo\":{\n"
            + "            \"bar\":{\n"
            + "                \"P357\":[\n"
            + "                            {\n"
            + "\n"
            + "                                \"datavalue\":{\n"
            + "                                    \"value\":\"\\\"\",\n"
            + "\n"
            + "                                }\n"
            + "                        }\n"
            + "                ]   \n"
            + "            },\n"
            + "            \"three\": \"a\"\n"
            + "        }\n"
            + "} ";

    doc.fromJSON(sb);
    Assert.assertEquals(doc.field("foo.three"), "a");
    final Collection c = doc.field("foo.bar.P357");
    Assert.assertEquals(c.size(), 1);
    final Map doc2 = (Map) c.iterator().next();
    Assert.assertEquals(((Map) doc2.get("datavalue")).get("value"), "\"");
  }

  public void testEscapingDoubleQuotes3() {
    final YTDocument doc = new YTDocument();
    String sb =
        " {\n"
            + "    \"foo\":{\n"
            + "            \"bar\":{\n"
            + "                \"P357\":[\n"
            + "                            {\n"
            + "\n"
            + "                                \"datavalue\":{\n"
            + "                                    \"value\":\"\\\"\",\n"
            + "\n"
            + "                                }\n"
            + "                        }\n"
            + "                ]   \n"
            + "            }\n"
            + "        }\n"
            + "} ";

    doc.fromJSON(sb);
    final Collection c = doc.field("foo.bar.P357");
    Assert.assertEquals(c.size(), 1);
    final Map doc2 = (Map) c.iterator().next();
    Assert.assertEquals(((Map) doc2.get("datavalue")).get("value"), "\"");
  }

  public void testEmbeddedQuotes() {
    YTDocument doc = new YTDocument();
    // FROM ISSUE 3151
    doc.fromJSON("{\"mainsnak\":{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}}");
    Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Sub\\urban");
  }

  public void testEmbeddedQuotes2() {
    YTDocument doc = new YTDocument();
    doc.fromJSON("{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}");
    Assert.assertEquals(doc.field("datavalue.value"), "Sub\\urban");
  }

  public void testEmbeddedQuotes2a() {
    YTDocument doc = new YTDocument();
    doc.fromJSON("{\"datavalue\":\"Sub\\\\urban\"}");
    Assert.assertEquals(doc.field("datavalue"), "Sub\\urban");
  }

  public void testEmbeddedQuotes3() {
    final YTDocument doc = new YTDocument();
    doc.fromJSON("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\"\"}}}");
    Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Suburban\\\"");
  }

  public void testEmbeddedQuotes4() {
    final YTDocument doc = new YTDocument();
    doc.fromJSON("{\"datavalue\":{\"value\":\"Suburban\\\\\"\"}}");
    Assert.assertEquals(doc.field("datavalue.value"), "Suburban\\\"");
  }

  public void testEmbeddedQuotes5() {
    final YTDocument doc = new YTDocument();
    doc.fromJSON("{\"datavalue\":\"Suburban\\\\\"\"}");
    Assert.assertEquals(doc.field("datavalue"), "Suburban\\\"");
  }

  public void testEmbeddedQuotes6() {
    YTDocument doc = new YTDocument();
    doc.fromJSON("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\"}}}");
    Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Suburban\\");
  }

  public void testEmbeddedQuotes7() {
    YTDocument doc = new YTDocument();
    doc.fromJSON("{\"datavalue\":{\"value\":\"Suburban\\\\\"}}");
    Assert.assertEquals(doc.field("datavalue.value"), "Suburban\\");
  }

  public void testEmbeddedQuotes8() {
    YTDocument doc = new YTDocument();
    doc.fromJSON("{\"datavalue\":\"Suburban\\\\\"}");
    Assert.assertEquals(doc.field("datavalue"), "Suburban\\");
  }

  public void testEmpty() {
    YTDocument doc = new YTDocument();
    doc.fromJSON("{}");
    Assert.assertEquals(doc.fieldNames().length, 0);
  }

  public void testInvalidJson() {
    YTDocument doc = new YTDocument();
    try {
      doc.fromJSON("{");
      Assert.fail();
    } catch (YTSerializationException e) {
    }

    try {
      doc.fromJSON("{\"foo\":{}");
      Assert.fail();
    } catch (YTSerializationException e) {
    }

    try {
      doc.fromJSON("{{}");
      Assert.fail();
    } catch (YTSerializationException e) {
    }

    try {
      doc.fromJSON("{}}");
      Assert.fail();
    } catch (YTSerializationException e) {
    }

    try {
      doc.fromJSON("}");
      Assert.fail();
    } catch (YTSerializationException e) {
    }
  }

  public void testDates() {
    final Date now = new Date(1350518475000L);

    final YTDocument doc = new YTDocument();
    doc.field("date", now);
    final String json = doc.toJSON();

    final YTDocument unmarshalled = new YTDocument();
    unmarshalled.fromJSON(json);
    Assert.assertEquals(unmarshalled.field("date"), now);
  }

  @Test
  public void shouldDeserializeFieldWithCurlyBraces() {
    final String json = "{\"a\":\"{dd}\",\"bl\":{\"b\":\"c\",\"a\":\"d\"}}";
    final YTDocument in =
        (YTDocument)
            ORecordSerializerJSON.INSTANCE.fromString(database,
                json, database.newInstance(), new String[]{});
    Assert.assertEquals(in.field("a"), "{dd}");
    Assert.assertTrue(in.field("bl") instanceof Map);
  }

  @Test
  public void testList() throws Exception {
    YTDocument documentSource = new YTDocument();
    documentSource.fromJSON("{\"list\" : [\"string\", 42]}");

    YTDocument documentTarget = new YTDocument();
    ORecordInternal.unsetDirty(documentTarget);
    documentTarget.fromStream(documentSource.toStream());

    OTrackedList<Object> list = documentTarget.field("list", YTType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), "string");
    Assert.assertEquals(list.get(1), 42);
  }

  @Test
  public void testEmbeddedRIDBagDeserialisationWhenFieldTypeIsProvided() throws Exception {
    YTDocument documentSource = new YTDocument();
    documentSource.fromJSON(
        "{FirstName:\"Student A"
            + " 0\",in_EHasGoodStudents:[#57:0],@fieldTypes:\"in_EHasGoodStudents=g\"}");

    ORidBag bag = documentSource.field("in_EHasGoodStudents");
    Assert.assertEquals(bag.size(), 1);
    YTIdentifiable rid = bag.iterator().next();
    Assert.assertEquals(rid.getIdentity().getClusterId(), 57);
    Assert.assertEquals(rid.getIdentity().getClusterPosition(), 0);
  }

  public void testNestedLinkCreation() {
    YTDocument jaimeDoc = new YTDocument("NestedLinkCreation");
    jaimeDoc.field("name", "jaime");

    database.begin();
    jaimeDoc.save();
    database.commit();

    jaimeDoc = database.bindToSession(jaimeDoc);
    // The link between jaime and cersei is saved properly - the #2263 test case
    YTDocument cerseiDoc = new YTDocument("NestedLinkCreation");
    cerseiDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"cersei\",\"valonqar\":" + jaimeDoc.toJSON() + "}");
    database.begin();
    cerseiDoc.save();
    database.commit();

    jaimeDoc = database.bindToSession(jaimeDoc);
    // The link between jamie and tyrion is not saved properly
    YTDocument tyrionDoc = new YTDocument("NestedLinkCreation");
    tyrionDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\","
            + " \"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.toJSON()
            + "}}");

    database.begin();
    tyrionDoc.save();
    database.commit();

    final Map<YTRID, YTDocument> contentMap = new HashMap<YTRID, YTDocument>();

    YTDocument jaime = new YTDocument("NestedLinkCreation");
    jaime.field("name", "jaime");

    contentMap.put(jaimeDoc.getIdentity(), jaime);

    YTDocument cersei = new YTDocument("NestedLinkCreation");
    cersei.field("name", "cersei");
    cersei.field("valonqar", jaimeDoc.getIdentity());
    contentMap.put(cerseiDoc.getIdentity(), cersei);

    YTDocument tyrion = new YTDocument("NestedLinkCreation");
    tyrion.field("name", "tyrion");

    YTDocument embeddedDoc = new YTDocument();
    embeddedDoc.field("relationship", "brother");
    embeddedDoc.field("contact", jaimeDoc.getIdentity());
    tyrion.field("emergency_contact", embeddedDoc);

    contentMap.put(tyrionDoc.getIdentity(), tyrion);

    final Map<YTRID, List<YTRID>> traverseMap = new HashMap<YTRID, List<YTRID>>();
    List<YTRID> jaimeTraverse = new ArrayList<YTRID>();
    jaimeTraverse.add(jaimeDoc.getIdentity());
    traverseMap.put(jaimeDoc.getIdentity(), jaimeTraverse);

    List<YTRID> cerseiTraverse = new ArrayList<YTRID>();
    cerseiTraverse.add(cerseiDoc.getIdentity());
    cerseiTraverse.add(jaimeDoc.getIdentity());

    traverseMap.put(cerseiDoc.getIdentity(), cerseiTraverse);

    List<YTRID> tyrionTraverse = new ArrayList<YTRID>();
    tyrionTraverse.add(tyrionDoc.getIdentity());
    tyrionTraverse.add(jaimeDoc.getIdentity());
    traverseMap.put(tyrionDoc.getIdentity(), tyrionTraverse);

    for (YTDocument o : database.browseClass("NestedLinkCreation")) {
      YTDocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));

      List<YTRID> traverse = traverseMap.remove(o.getIdentity());
      for (YTIdentifiable id :
          new OSQLSynchQuery<YTDocument>("traverse * from " + o.getIdentity().toString())) {
        Assert.assertTrue(traverse.remove(id.getIdentity()));
      }

      Assert.assertTrue(traverse.isEmpty());
    }

    Assert.assertTrue(traverseMap.isEmpty());
  }

  public void testNestedLinkCreationFieldTypes() {
    YTDocument jaimeDoc = new YTDocument("NestedLinkCreationFieldTypes");
    jaimeDoc.field("name", "jaime");

    database.begin();
    jaimeDoc.save();
    database.commit();

    // The link between jaime and cersei is saved properly - the #2263 test case
    YTDocument cerseiDoc = new YTDocument("NestedLinkCreationFieldTypes");
    cerseiDoc.fromJSON(
        "{\"@type\":\"d\",\"@fieldTypes\":\"valonqar=x\",\"name\":\"cersei\",\"valonqar\":"
            + jaimeDoc.getIdentity()
            + "}");

    database.begin();
    cerseiDoc.save();
    database.commit();

    // The link between jamie and tyrion is not saved properly
    YTDocument tyrionDoc = new YTDocument("NestedLinkCreationFieldTypes");
    tyrionDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\","
            + " \"@fieldTypes\":\"contact=x\",\"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.getIdentity()
            + "}}");

    database.begin();
    tyrionDoc.save();
    database.commit();

    final Map<YTRID, YTDocument> contentMap = new HashMap<YTRID, YTDocument>();

    YTDocument jaime = new YTDocument("NestedLinkCreationFieldTypes");
    jaime.field("name", "jaime");

    contentMap.put(jaimeDoc.getIdentity(), jaime);

    YTDocument cersei = new YTDocument("NestedLinkCreationFieldTypes");
    cersei.field("name", "cersei");
    cersei.field("valonqar", jaimeDoc.getIdentity());
    contentMap.put(cerseiDoc.getIdentity(), cersei);

    YTDocument tyrion = new YTDocument("NestedLinkCreationFieldTypes");
    tyrion.field("name", "tyrion");

    YTDocument embeddedDoc = new YTDocument();
    embeddedDoc.field("relationship", "brother");
    embeddedDoc.field("contact", jaimeDoc.getIdentity());
    tyrion.field("emergency_contact", embeddedDoc);

    contentMap.put(tyrionDoc.getIdentity(), tyrion);

    final Map<YTRID, List<YTRID>> traverseMap = new HashMap<YTRID, List<YTRID>>();
    List<YTRID> jaimeTraverse = new ArrayList<YTRID>();
    jaimeTraverse.add(jaimeDoc.getIdentity());
    traverseMap.put(jaimeDoc.getIdentity(), jaimeTraverse);

    List<YTRID> cerseiTraverse = new ArrayList<YTRID>();
    cerseiTraverse.add(cerseiDoc.getIdentity());
    cerseiTraverse.add(jaimeDoc.getIdentity());

    traverseMap.put(cerseiDoc.getIdentity(), cerseiTraverse);

    List<YTRID> tyrionTraverse = new ArrayList<YTRID>();
    tyrionTraverse.add(tyrionDoc.getIdentity());
    tyrionTraverse.add(jaimeDoc.getIdentity());
    traverseMap.put(tyrionDoc.getIdentity(), tyrionTraverse);

    for (YTDocument o : database.browseClass("NestedLinkCreationFieldTypes")) {
      YTDocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));

      List<YTRID> traverse = traverseMap.remove(o.getIdentity());
      for (YTIdentifiable id :
          new OSQLSynchQuery<YTDocument>("traverse * from " + o.getIdentity().toString())) {
        Assert.assertTrue(traverse.remove(id.getIdentity()));
      }
      Assert.assertTrue(traverse.isEmpty());
    }
    Assert.assertTrue(traverseMap.isEmpty());
  }

  public void testInnerDocCreation() {
    YTDocument adamDoc = new YTDocument("InnerDocCreation");
    adamDoc.fromJSON("{\"name\":\"adam\"}");

    database.begin();
    adamDoc.save();
    database.commit();

    database.begin();
    adamDoc = database.bindToSession(adamDoc);
    YTDocument eveDoc = new YTDocument("InnerDocCreation");
    eveDoc.fromJSON("{\"@type\":\"d\",\"name\":\"eve\",\"friends\":[" + adamDoc.toJSON() + "]}");

    eveDoc.save();
    database.commit();

    Map<YTRID, YTDocument> contentMap = new HashMap<YTRID, YTDocument>();
    YTDocument adam = new YTDocument("InnerDocCreation");
    adam.field("name", "adam");

    contentMap.put(adamDoc.getIdentity(), adam);

    YTDocument eve = new YTDocument("InnerDocCreation");
    eve.field("name", "eve");

    List<YTRID> friends = new ArrayList<YTRID>();
    friends.add(adamDoc.getIdentity());
    eve.field("friends", friends);

    contentMap.put(eveDoc.getIdentity(), eve);

    Map<YTRID, List<YTRID>> traverseMap = new HashMap<YTRID, List<YTRID>>();

    List<YTRID> adamTraverse = new ArrayList<YTRID>();
    adamTraverse.add(adamDoc.getIdentity());
    traverseMap.put(adamDoc.getIdentity(), adamTraverse);

    List<YTRID> eveTraverse = new ArrayList<YTRID>();
    eveTraverse.add(eveDoc.getIdentity());
    eveTraverse.add(adamDoc.getIdentity());

    traverseMap.put(eveDoc.getIdentity(), eveTraverse);

    for (YTDocument o : database.browseClass("InnerDocCreation")) {
      YTDocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));
    }

    for (final YTDocument o : database.browseClass("InnerDocCreation")) {
      final List<YTRID> traverse = traverseMap.remove(o.getIdentity());
      for (final YTIdentifiable id :
          new OSQLSynchQuery<YTDocument>("traverse * from " + o.getIdentity().toString())) {
        Assert.assertTrue(traverse.remove(id.getIdentity()));
      }
      Assert.assertTrue(traverse.isEmpty());
    }
    Assert.assertTrue(traverseMap.isEmpty());
  }

  public void testInnerDocCreationFieldTypes() {
    YTDocument adamDoc = new YTDocument("InnerDocCreationFieldTypes");
    adamDoc.fromJSON("{\"name\":\"adam\"}");

    database.begin();
    adamDoc.save();
    database.commit();

    YTDocument eveDoc = new YTDocument("InnerDocCreationFieldTypes");
    eveDoc.fromJSON(
        "{\"@type\":\"d\", \"@fieldTypes\" : \"friends=z\", \"name\":\"eve\",\"friends\":["
            + adamDoc.getIdentity()
            + "]}");

    database.begin();
    eveDoc.save();
    database.commit();

    Map<YTRID, YTDocument> contentMap = new HashMap<YTRID, YTDocument>();
    YTDocument adam = new YTDocument("InnerDocCreationFieldTypes");
    adam.field("name", "adam");

    contentMap.put(adamDoc.getIdentity(), adam);

    YTDocument eve = new YTDocument("InnerDocCreationFieldTypes");
    eve.field("name", "eve");

    List<YTRID> friends = new ArrayList<YTRID>();
    friends.add(adamDoc.getIdentity());
    eve.field("friends", friends);

    contentMap.put(eveDoc.getIdentity(), eve);

    Map<YTRID, List<YTRID>> traverseMap = new HashMap<YTRID, List<YTRID>>();

    List<YTRID> adamTraverse = new ArrayList<YTRID>();
    adamTraverse.add(adamDoc.getIdentity());
    traverseMap.put(adamDoc.getIdentity(), adamTraverse);

    List<YTRID> eveTraverse = new ArrayList<YTRID>();
    eveTraverse.add(eveDoc.getIdentity());
    eveTraverse.add(adamDoc.getIdentity());

    traverseMap.put(eveDoc.getIdentity(), eveTraverse);

    for (YTDocument o : database.browseClass("InnerDocCreationFieldTypes")) {
      YTDocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));
    }

    for (YTDocument o : database.browseClass("InnerDocCreationFieldTypes")) {
      List<YTRID> traverse = traverseMap.remove(o.getIdentity());
      for (YTIdentifiable id :
          new OSQLSynchQuery<YTDocument>("traverse * from " + o.getIdentity().toString())) {
        Assert.assertTrue(traverse.remove(id.getIdentity()));
      }

      Assert.assertTrue(traverse.isEmpty());
    }

    Assert.assertTrue(traverseMap.isEmpty());
  }

  public void testJSONTxDocInsertOnly() {
    final String classNameDocOne = "JSONTxDocOneInsertOnly";
    if (!database.getMetadata().getSchema().existsClass(classNameDocOne)) {
      database.getMetadata().getSchema().createClass(classNameDocOne);
    }
    final String classNameDocTwo = "JSONTxDocTwoInsertOnly";
    if (!database.getMetadata().getSchema().existsClass(classNameDocTwo)) {
      database.getMetadata().getSchema().createClass(classNameDocTwo);
    }
    database.begin();
    final YTDocument eveDoc = new YTDocument(classNameDocOne);
    eveDoc.field("name", "eve");
    eveDoc.save();

    final YTDocument nestedWithTypeD = new YTDocument(classNameDocTwo);
    nestedWithTypeD.fromJSON(
        "{\"@type\":\"d\",\"event_name\":\"world cup 2014\",\"admin\":[" + eveDoc.toJSON() + "]}");
    nestedWithTypeD.save();
    database.commit();
    Assert.assertEquals(database.countClass(classNameDocOne), 1);

    final Map<YTRID, YTDocument> contentMap = new HashMap<>();
    final YTDocument eve = new YTDocument(classNameDocOne);
    eve.field("name", "eve");
    contentMap.put(eveDoc.getIdentity(), eve);

    for (final YTDocument document : database.browseClass(classNameDocOne)) {
      final YTDocument content = contentMap.get(document.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(document));
    }
  }

  public void testJSONTxDoc() {
    if (!database.getMetadata().getSchema().existsClass("JSONTxDocOne")) {
      database.getMetadata().getSchema().createClass("JSONTxDocOne");
    }

    if (!database.getMetadata().getSchema().existsClass("JSONTxDocTwo")) {
      database.getMetadata().getSchema().createClass("JSONTxDocTwo");
    }

    YTDocument adamDoc = new YTDocument("JSONTxDocOne");
    adamDoc.field("name", "adam");

    database.begin();
    adamDoc.save();
    database.commit();

    database.begin();
    YTDocument eveDoc = new YTDocument("JSONTxDocOne");
    eveDoc.field("name", "eve");
    eveDoc.save();

    adamDoc = database.bindToSession(adamDoc);
    final YTDocument nestedWithTypeD = new YTDocument("JSONTxDocTwo");
    nestedWithTypeD.fromJSON(
        "{\"@type\":\"d\",\"event_name\":\"world cup 2014\",\"admin\":["
            + eveDoc.toJSON()
            + ","
            + adamDoc.toJSON()
            + "]}");
    nestedWithTypeD.save();

    database.commit();

    Assert.assertEquals(database.countClass("JSONTxDocOne"), 2);

    Map<YTRID, YTDocument> contentMap = new HashMap<>();
    YTDocument adam = new YTDocument("JSONTxDocOne");
    adam.field("name", "adam");
    contentMap.put(adamDoc.getIdentity(), adam);

    YTDocument eve = new YTDocument("JSONTxDocOne");
    eve.field("name", "eve");
    contentMap.put(eveDoc.getIdentity(), eve);

    for (YTDocument o : database.browseClass("JSONTxDocOne")) {
      YTDocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));
    }
  }

  public void testInvalidLink() {
    YTDocument nullRefDoc = new YTDocument();
    nullRefDoc.fromJSON("{\"name\":\"Luca\", \"ref\":\"#-1:-1\"}");
    // Assert.assertNull(nullRefDoc.rawField("ref"));

    String json = nullRefDoc.toJSON();
    int pos = json.indexOf("\"ref\":");

    Assert.assertTrue(pos > -1);
    Assert.assertEquals(json.charAt(pos + "\"ref\":".length()), 'n');
  }

  public void testOtherJson() {
    new YTDocument()
        .fromJSON(
            "{\"Salary\":1500.0,\"Type\":\"Person\",\"Address\":[{\"Zip\":\"JX2"
                + " MSX\",\"Type\":\"Home\",\"Street1\":\"13 Marge"
                + " Street\",\"Country\":\"Holland\",\"Id\":\"Address-28813211\",\"City\":\"Amsterdam\",\"From\":\"1996-02-01\",\"To\":\"1998-01-01\"},{\"Zip\":\"90210\",\"Type\":\"Work\",\"Street1\":\"100"
                + " Hollywood"
                + " Drive\",\"Country\":\"USA\",\"Id\":\"Address-11595040\",\"City\":\"Los"
                + " Angeles\",\"From\":\"2009-09-01\"}],\"Id\":\"Person-7464251\",\"Name\":\"Stan\"}");
  }

  @Test
  public void testScientificNotation() {
    final YTDocument doc = new YTDocument();
    doc.fromJSON("{'number1': -9.2741500e-31, 'number2': 741800E+290}");

    final double number1 = doc.field("number1");
    Assert.assertEquals(number1, -9.27415E-31);
    final double number2 = doc.field("number2");
    Assert.assertEquals(number2, 741800E+290);
  }
}
