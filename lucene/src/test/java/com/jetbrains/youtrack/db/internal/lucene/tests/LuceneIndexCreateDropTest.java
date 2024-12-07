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

package com.jetbrains.youtrack.db.internal.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneIndexCreateDropTest extends LuceneBaseTest {

  public LuceneIndexCreateDropTest() {
  }

  @Before
  public void init() {
    SchemaClass type = db.createVertexClass("City");
    type.createProperty(db, "name", PropertyType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void dropIndex() {

    db.command("drop index City.name");

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.name");

    assertThat(index).isNull();
  }
}
