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

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class LuceneFacetTest extends LuceneBaseTest {

  @Before
  public void init() {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass oClass = schema.createClass("Item");

    oClass.createProperty(db, "name", PropertyType.STRING);
    oClass.createProperty(db, "category", PropertyType.STRING);

    db.command(
            "create index Item.name_category on Item (name,category) FULLTEXT ENGINE LUCENE"
                + " METADATA { 'facetFields' : ['category']}")
        .close();

    EntityImpl doc = ((EntityImpl) db.newEntity("Item"));
    doc.field("name", "Pioneer");
    doc.field("category", "Electronic/HiFi");

    db.save(doc);

    doc = ((EntityImpl) db.newEntity("Item"));
    doc.field("name", "Hitachi");
    doc.field("category", "Electronic/HiFi");

    db.save(doc);

    doc = ((EntityImpl) db.newEntity("Item"));
    doc.field("name", "Philips");
    doc.field("category", "Electronic/HiFi");

    db.save(doc);

    doc = ((EntityImpl) db.newEntity("Item"));
    doc.field("name", "HP");
    doc.field("category", "Electronic/Computer");

    db.save(doc);

    db.commit();
  }

  @Test
  @Ignore
  public void baseFacetTest() {

    var resultSet =
        db.command("select *,$facet from Item where name lucene '(name:P*)' limit 1 ").toList();

    Assert.assertEquals(1, resultSet.size());

    List<EntityImpl> facets = resultSet.getFirst().getProperty("$facet");

    Assert.assertEquals(1, facets.size());

    EntityImpl facet = facets.getFirst();
    Assert.assertEquals(1, facet.<Object>field("childCount"));
    Assert.assertEquals(2, facet.<Object>field("value"));
    Assert.assertEquals("category", facet.field("dim"));

    List<EntityImpl> labelsValues = facet.field("labelsValue");

    Assert.assertEquals(1, labelsValues.size());

    EntityImpl labelValues = labelsValues.getFirst();

    Assert.assertEquals(2, labelValues.<Object>field("value"));
    Assert.assertEquals("Electronic", labelValues.field("label"));

    resultSet =
        db
            .command(
                "select *,$facet from Item where name lucene { 'q' : 'H*', 'drillDown' :"
                    + " 'category:Electronic' }  limit 1 ").toList();

    Assert.assertEquals(1, resultSet.size());

    facets = resultSet.getFirst().getProperty("$facet");

    Assert.assertEquals(1, facets.size());

    facet = facets.getFirst();

    Assert.assertEquals(2, facet.<Object>field("childCount"));
    Assert.assertEquals(2, facet.<Object>field("value"));
    Assert.assertEquals("category", facet.field("dim"));

    labelsValues = facet.field("labelsValue");

    Assert.assertEquals(2, labelsValues.size());

    labelValues = labelsValues.getFirst();

    Assert.assertEquals(1, labelValues.<Object>field("value"));
    Assert.assertEquals("HiFi", labelValues.field("label"));

    labelValues = labelsValues.get(1);

    Assert.assertEquals(1, labelValues.<Object>field("value"));
    Assert.assertEquals("Computer", labelValues.field("label"));
  }
}
