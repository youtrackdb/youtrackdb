/*
 *
 *
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneCreateJavaApiTest extends BaseLuceneTest {

  public static final String SONG_CLASS = "Song";

  @Before
  public void init() {
    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass v = schema.getClass("V");
    final YTClass song = schema.createClass(SONG_CLASS);
    song.setSuperClass(db, v);
    song.createProperty(db, "title", YTType.STRING);
    song.createProperty(db, "author", YTType.STRING);
    song.createProperty(db, "description", YTType.STRING);
  }

  @Test
  public void testCreateIndex() {
    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass song = schema.getClass(SONG_CLASS);

    final YTDocument meta = new YTDocument().field("analyzer", StandardAnalyzer.class.getName());
    final OIndex lucene =
        song.createIndex(db,
            "Song.title",
            YTClass.INDEX_TYPE.FULLTEXT.toString(),
            null,
            meta,
            "LUCENE", new String[]{"title"});

    assertThat(lucene).isNotNull();
    assertThat(lucene.getMetadata().containsKey("analyzer")).isTrue();
    assertThat(lucene.getMetadata().get("analyzer"))
        .isEqualTo(StandardAnalyzer.class.getName());
  }

  @Test
  public void testCreateIndexCompositeWithDefaultAnalyzer() {
    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass song = schema.getClass(SONG_CLASS);
    final OIndex lucene =
        song.createIndex(db,
            "Song.author_description",
            YTClass.INDEX_TYPE.FULLTEXT.toString(),
            null,
            null,
            "LUCENE", new String[]{"author", "description"});

    assertThat(lucene).isNotNull();
    assertThat(lucene.getMetadata().containsKey("analyzer")).isTrue();
    assertThat(lucene.getMetadata().get("analyzer"))
        .isEqualTo(StandardAnalyzer.class.getName());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreateIndexWithUnsupportedEmbedded() {
    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass song = schema.getClass(SONG_CLASS);
    song.createProperty(db, YTType.EMBEDDED.getName(), YTType.EMBEDDED);
    song.createIndex(db,
        SONG_CLASS + "." + YTType.EMBEDDED.getName(),
        YTClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE", new String[]{"description", YTType.EMBEDDED.getName()});
    Assert.assertEquals(1, song.getIndexes(db).size());
  }

  @Test
  public void testCreateIndexEmbeddedMapJSON() {
    db.begin();
    var songDoc = new YTDocument(SONG_CLASS);
    songDoc.fromJSON(
        "{\n"
            + "    \"description\": \"Capital\",\n"
            + "    \"String"
            + YTType.EMBEDDEDMAP.getName()
            + "\": {\n"
            + "    \"text\": \"Hello Rome how are you today?\",\n"
            + "    \"text2\": \"Hello Bolzano how are you today?\",\n"
            + "    }\n"
            + "}");
    db.save(songDoc);
    db.commit();
    final YTClass song = createEmbeddedMapIndex();
    checkCreatedEmbeddedMapIndex(song, "LUCENE");

    queryIndexEmbeddedMapClass("Bolzano", 1);
  }

  @Test
  public void testCreateIndexEmbeddedMapApi() {
    addDocumentViaAPI();

    final YTClass song = createEmbeddedMapIndex();
    checkCreatedEmbeddedMapIndex(song, "LUCENE");

    queryIndexEmbeddedMapClass("Bolzano", 1);
  }

  @Test
  public void testCreateIndexEmbeddedMapApiSimpleTree() {
    addDocumentViaAPI();

    final YTClass song = createEmbeddedMapIndexSimple();
    checkCreatedEmbeddedMapIndex(song, "CELL_BTREE");

    queryIndexEmbeddedMapClass("Hello Bolzano how are you today?", 0);
  }

  private void addDocumentViaAPI() {
    final Map<String, String> entries = new HashMap<>();
    entries.put("text", "Hello Rome how are you today?");
    entries.put("text2", "Hello Bolzano how are you today?");

    final YTDocument doc = new YTDocument(SONG_CLASS);
    doc.field("description", "Capital", YTType.STRING);
    doc.field("String" + YTType.EMBEDDEDMAP.getName(), entries, YTType.EMBEDDEDMAP, YTType.STRING);
    db.begin();
    db.save(doc);
    db.commit();
  }

  @Test
  public void testCreateIndexEmbeddedMapApiSimpleDoesNotReturnResult() {
    addDocumentViaAPI();

    final YTClass song = createEmbeddedMapIndexSimple();
    checkCreatedEmbeddedMapIndex(song, "CELL_BTREE");

    queryIndexEmbeddedMapClass("Bolzano", 0);
  }

  private void queryIndexEmbeddedMapClass(final String searchTerm, final int expectedCount) {
    final OResultSet result =
        db.query(
            "select from "
                + SONG_CLASS
                + " where SEARCH_CLASS('"
                + searchTerm
                + "', {\n"
                + "    \"allowLeadingWildcard\": true ,\n"
                + "    \"lowercaseExpandedTerms\": true\n"
                + "}) = true");
    Assert.assertEquals(expectedCount, result.stream().count());
  }

  private void checkCreatedEmbeddedMapIndex(final YTClass clazz, final String expectedAlgorithm) {
    final OIndex index = clazz.getIndexes(db).iterator().next();
    System.out.println(
        "key-name: " + ((OIndexInternal) index).getIndexId() + "-" + index.getName());

    Assert.assertEquals("index algorithm", expectedAlgorithm, index.getAlgorithm());
    Assert.assertEquals("index type", "FULLTEXT", index.getType());
    Assert.assertEquals("Key type", YTType.STRING, index.getKeyTypes()[0]);
    Assert.assertEquals(
        "Definition field", "StringEmbeddedMap", index.getDefinition().getFields().get(0));
    Assert.assertEquals(
        "Definition field to index",
        "StringEmbeddedMap by value",
        index.getDefinition().getFieldsToIndex().get(0));
    Assert.assertEquals("Definition type", YTType.STRING, index.getDefinition().getTypes()[0]);
  }

  private YTClass createEmbeddedMapIndex() {
    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass song = schema.getClass(SONG_CLASS);
    song.createProperty(db, "String" + YTType.EMBEDDEDMAP.getName(), YTType.EMBEDDEDMAP,
        YTType.STRING);
    song.createIndex(db,
        SONG_CLASS + "." + YTType.EMBEDDEDMAP.getName(),
        YTClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE", new String[]{"String" + YTType.EMBEDDEDMAP.getName() + " by value"});
    Assert.assertEquals(1, song.getIndexes(db).size());
    return song;
  }

  private YTClass createEmbeddedMapIndexSimple() {
    final YTSchema schema = db.getMetadata().getSchema();
    final YTClass song = schema.getClass(SONG_CLASS);
    song.createProperty(db, "String" + YTType.EMBEDDEDMAP.getName(), YTType.EMBEDDEDMAP,
        YTType.STRING);
    song.createIndex(db,
        SONG_CLASS + "." + YTType.EMBEDDEDMAP.getName(),
        YTClass.INDEX_TYPE.FULLTEXT.toString(),
        "String" + YTType.EMBEDDEDMAP.getName() + " by value");
    Assert.assertEquals(1, song.getIndexes(db).size());
    return song;
  }
}
