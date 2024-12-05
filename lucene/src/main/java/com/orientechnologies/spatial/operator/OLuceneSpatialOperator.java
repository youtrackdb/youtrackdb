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
package com.orientechnologies.spatial.operator;

import com.orientechnologies.lucene.operator.OLuceneOperatorUtil;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.core.sql.OIndexSearchResult;
import com.orientechnologies.core.sql.OSQLEngine;
import com.orientechnologies.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.core.sql.functions.OSQLFunction;
import com.orientechnologies.core.sql.operator.OIndexReuseType;
import com.orientechnologies.core.sql.operator.OQueryTargetOperator;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.util.List;

/**
 *
 */
public abstract class OLuceneSpatialOperator extends OQueryTargetOperator {

  protected OShapeBuilder factory;

  protected OLuceneSpatialOperator(String iKeyword, int iPrecedence, boolean iLogical) {
    super(iKeyword, iPrecedence, iLogical);
    factory = OShapeFactory.INSTANCE;
  }

  @Override
  public OIndexSearchResult getOIndexSearchResult(
      YTClass iSchemaClass,
      OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults,
      OCommandContext context) {
    return OLuceneOperatorUtil.buildOIndexSearchResult(
        iSchemaClass, iCondition, iIndexSearchResults, context);
  }

  // TODO HANDLE EVALUATE RECORD
  @Override
  public Object evaluateRecord(
      YTIdentifiable iRecord,
      YTEntityImpl iCurrentResult,
      OSQLFilterCondition iCondition,
      Object iLeft,
      Object iRight,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {

    OSQLFunction function = OSQLEngine.getInstance().getFunction(iContext.getDatabase(), keyword);
    return function.execute(
        this, iRecord, iCurrentResult, new Object[]{iLeft, iCondition.getRight()}, iContext);
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return OIndexReuseType.INDEX_OPERATOR;
  }

  @Override
  public YTRID getBeginRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public YTRID getEndRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }
}
