/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.orientechnologies.spatial.factory;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.index.OIndexDefinition;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.bbox.BBoxStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.locationtech.spatial4j.context.SpatialContext;

/**
 *
 */
public class OSpatialStrategyFactory {

  private final OShapeBuilder factory;

  public OSpatialStrategyFactory(OShapeBuilder factory) {
    this.factory = factory;
  }

  public static SpatialStrategy createStrategy(
      SpatialContext ctx,
      YTDatabaseSessionInternal db,
      OIndexDefinition indexDefinition) {

    YTClass aClass =
        db.getMetadata().getImmutableSchemaSnapshot().getClass(indexDefinition.getClassName());

    YTProperty property = aClass.getProperty(indexDefinition.getFields().get(0));

    YTClass linkedClass = property.getLinkedClass();

    if ("OPoint".equalsIgnoreCase(linkedClass.getName())) {
      RecursivePrefixTreeStrategy strategy =
          new RecursivePrefixTreeStrategy(new GeohashPrefixTree(ctx, 11), "location");
      strategy.setDistErrPct(0);
      return strategy;
    }
    return BBoxStrategy.newInstance(ctx, "location");
  }
}
