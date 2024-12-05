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

package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneCreateJavaApiTest extends OLuceneBaseTest {

  @Before
  public void init() {
    YTClass song = db.createVertexClass("Song");
    song.createProperty(db, "title", YTType.STRING);
    song.createProperty(db, "author", YTType.STRING);
    song.createProperty(db, "description", YTType.STRING);
  }

  @Test
  public void testCreateIndex() {
    YTSchema schema = db.getMetadata().getSchema();

    YTClass song = schema.getClass("Song");

    EntityImpl meta = new EntityImpl().field("analyzer", StandardAnalyzer.class.getName());
    OIndex lucene =
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
    YTSchema schema = db.getMetadata().getSchema();

    YTClass song = schema.getClass("Song");

    OIndex lucene =
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
}
