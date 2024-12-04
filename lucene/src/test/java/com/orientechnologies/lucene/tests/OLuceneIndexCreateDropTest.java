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

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneIndexCreateDropTest extends OLuceneBaseTest {

  public OLuceneIndexCreateDropTest() {
  }

  @Before
  public void init() {
    YTClass type = db.createVertexClass("City");
    type.createProperty(db, "name", YTType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void dropIndex() {

    db.command("drop index City.name");

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.name");

    assertThat(index).isNull();
  }
}
