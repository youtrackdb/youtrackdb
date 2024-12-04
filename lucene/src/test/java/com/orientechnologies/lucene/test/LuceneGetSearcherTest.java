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

import com.orientechnologies.lucene.index.OLuceneIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneGetSearcherTest extends BaseLuceneTest {

  @Before
  public void init() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    YTClass song = schema.createClass("Person");
    song.setSuperClass(db, v);
    song.createProperty(db, "isDeleted", YTType.BOOLEAN);

    db.command("create index Person.isDeleted on Person (isDeleted) FULLTEXT ENGINE LUCENE")
        .close();
  }

  @Test
  public void testSearcherInstance() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Person.isDeleted");

    Assert.assertTrue(index.getInternal() instanceof OLuceneIndexNotUnique);

    OLuceneIndexNotUnique idx = (OLuceneIndexNotUnique) index.getInternal();

    Assert.assertNotNull(idx.searcher());
  }
}
