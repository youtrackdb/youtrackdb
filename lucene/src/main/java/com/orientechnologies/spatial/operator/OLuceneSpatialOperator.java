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

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.OIndexSearchResult;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunction;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OIndexReuseType;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OQueryTargetOperator;
import com.orientechnologies.lucene.operator.OLuceneOperatorUtil;
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
      CommandContext context) {
    return OLuceneOperatorUtil.buildOIndexSearchResult(
        iSchemaClass, iCondition, iIndexSearchResults, context);
  }

  // TODO HANDLE EVALUATE RECORD
  @Override
  public Object evaluateRecord(
      YTIdentifiable iRecord,
      EntityImpl iCurrentResult,
      OSQLFilterCondition iCondition,
      Object iLeft,
      Object iRight,
      CommandContext iContext,
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
