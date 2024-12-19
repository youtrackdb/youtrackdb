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

package com.jetbrains.youtrack.db.internal.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass v = schema.getClass("V");
    final SchemaClass song = schema.createClass(SONG_CLASS);
    song.setSuperClass(db, v);
    song.createProperty(db, "title", PropertyType.STRING);
    song.createProperty(db, "author", PropertyType.STRING);
    song.createProperty(db, "description", PropertyType.STRING);
  }

  @Test
  public void testCreateIndex() {
    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass song = schema.getClass(SONG_CLASS);

    var meta = Map.of("analyzer",
        StandardAnalyzer.class.getName());

    song.createIndex(db,
        "Song.title",
        SchemaClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        meta,
        "LUCENE", new String[]{"title"});
    var lucene = db.getIndex("Song.title");
    assertThat(lucene).isNotNull();
    assertThat(lucene.getMetadata().containsKey("analyzer")).isTrue();
    assertThat(lucene.getMetadata().get("analyzer"))
        .isEqualTo(StandardAnalyzer.class.getName());
  }

  @Test
  public void testCreateIndexCompositeWithDefaultAnalyzer() {
    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass song = schema.getClass(SONG_CLASS);

    song.createIndex(db,
        "Song.author_description",
        SchemaClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE", new String[]{"author", "description"});
    final Index lucene = db.getIndex("Song.author_description");

    assertThat(lucene).isNotNull();
    assertThat(lucene.getMetadata().containsKey("analyzer")).isTrue();
    assertThat(lucene.getMetadata().get("analyzer"))
        .isEqualTo(StandardAnalyzer.class.getName());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreateIndexWithUnsupportedEmbedded() {
    var schema = db.getMetadata().getSchema();
    var song = schema.getClassInternal(SONG_CLASS);
    song.createProperty(db, PropertyType.EMBEDDED.getName(), PropertyType.EMBEDDED);
    song.createIndex(db,
        SONG_CLASS + "." + PropertyType.EMBEDDED.getName(),
        SchemaClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE", new String[]{"description", PropertyType.EMBEDDED.getName()});
    Assert.assertEquals(1, song.getIndexes(db).size());
  }

  @Test
  public void testCreateIndexEmbeddedMapJSON() {
    db.begin();
    var songDoc = ((EntityImpl) db.newEntity(SONG_CLASS));
    songDoc.fromJSON(
        "{\n"
            + "    \"description\": \"Capital\",\n"
            + "    \"String"
            + PropertyType.EMBEDDEDMAP.getName()
            + "\": {\n"
            + "    \"text\": \"Hello Rome how are you today?\",\n"
            + "    \"text2\": \"Hello Bolzano how are you today?\",\n"
            + "    }\n"
            + "}");
    db.save(songDoc);
    db.commit();
    var song = createEmbeddedMapIndex();
    checkCreatedEmbeddedMapIndex(song, "LUCENE");

    queryIndexEmbeddedMapClass("Bolzano", 1);
  }

  @Test
  public void testCreateIndexEmbeddedMapApi() {
    addDocumentViaAPI();

    var song = createEmbeddedMapIndex();
    checkCreatedEmbeddedMapIndex(song, "LUCENE");

    queryIndexEmbeddedMapClass("Bolzano", 1);
  }

  @Test
  public void testCreateIndexEmbeddedMapApiSimpleTree() {
    addDocumentViaAPI();

    var song = createEmbeddedMapIndexSimple();
    checkCreatedEmbeddedMapIndex(song, "CELL_BTREE");

    queryIndexEmbeddedMapClass("Hello Bolzano how are you today?", 0);
  }

  private void addDocumentViaAPI() {
    final Map<String, String> entries = new HashMap<>();
    entries.put("text", "Hello Rome how are you today?");
    entries.put("text2", "Hello Bolzano how are you today?");

    final EntityImpl doc = ((EntityImpl) db.newEntity(SONG_CLASS));
    doc.field("description", "Capital", PropertyType.STRING);
    doc.field("String" + PropertyType.EMBEDDEDMAP.getName(), entries, PropertyType.EMBEDDEDMAP,
        PropertyType.STRING);
    db.begin();
    db.save(doc);
    db.commit();
  }

  @Test
  public void testCreateIndexEmbeddedMapApiSimpleDoesNotReturnResult() {
    addDocumentViaAPI();

    var song = createEmbeddedMapIndexSimple();
    checkCreatedEmbeddedMapIndex(song, "CELL_BTREE");

    queryIndexEmbeddedMapClass("Bolzano", 0);
  }

  private void queryIndexEmbeddedMapClass(final String searchTerm, final int expectedCount) {
    final ResultSet result =
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

  private void checkCreatedEmbeddedMapIndex(final SchemaClassInternal clazz,
      final String expectedAlgorithm) {
    final Index index = clazz.getIndexesInternal(db).iterator().next();
    System.out.println(
        "key-name: " + ((IndexInternal) index).getIndexId() + "-" + index.getName());

    Assert.assertEquals("index algorithm", expectedAlgorithm, index.getAlgorithm());
    Assert.assertEquals("index type", "FULLTEXT", index.getType());
    Assert.assertEquals("Key type", PropertyType.STRING, index.getKeyTypes()[0]);
    Assert.assertEquals(
        "Definition field", "StringEmbeddedMap", index.getDefinition().getFields().get(0));
    Assert.assertEquals(
        "Definition field to index",
        "StringEmbeddedMap by value",
        index.getDefinition().getFieldsToIndex().get(0));
    Assert.assertEquals("Definition type", PropertyType.STRING,
        index.getDefinition().getTypes()[0]);
  }

  private SchemaClassInternal createEmbeddedMapIndex() {
    var schema = db.getMetadata().getSchema();
    var song = schema.getClassInternal(SONG_CLASS);
    song.createProperty(db, "String" + PropertyType.EMBEDDEDMAP.getName(), PropertyType.EMBEDDEDMAP,
        PropertyType.STRING);
    song.createIndex(db,
        SONG_CLASS + "." + PropertyType.EMBEDDEDMAP.getName(),
        SchemaClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE", new String[]{"String" + PropertyType.EMBEDDEDMAP.getName() + " by value"});
    Assert.assertEquals(1, song.getIndexes(db).size());
    return song;
  }

  private SchemaClassInternal createEmbeddedMapIndexSimple() {
    var schema = db.getMetadata().getSchema();
    var song = schema.getClassInternal(SONG_CLASS);
    song.createProperty(db, "String" + PropertyType.EMBEDDEDMAP.getName(), PropertyType.EMBEDDEDMAP,
        PropertyType.STRING);
    song.createIndex(db,
        SONG_CLASS + "." + PropertyType.EMBEDDEDMAP.getName(),
        SchemaClass.INDEX_TYPE.FULLTEXT.toString(),
        "String" + PropertyType.EMBEDDEDMAP.getName() + " by value");
    Assert.assertEquals(1, song.getIndexes(db).size());
    return song;
  }
}
