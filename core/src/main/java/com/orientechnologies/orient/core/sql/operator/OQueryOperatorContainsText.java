/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import java.util.List;
import java.util.stream.Stream;

/**
 * CONTAINSTEXT operator. Look if a text is contained in a property. This is usually used with the
 * FULLTEXT-INDEX for fast lookup at piece of text.
 */
public class OQueryOperatorContainsText extends OQueryTargetOperator {

  private boolean ignoreCase = true;

  public OQueryOperatorContainsText(final boolean iIgnoreCase) {
    super("CONTAINSTEXT", 5, false);
    ignoreCase = iIgnoreCase;
  }

  public OQueryOperatorContainsText() {
    super("CONTAINSTEXT", 5, false);
  }

  @Override
  public String getSyntax() {
    return "<left> CONTAINSTEXT[( noignorecase ] )] <right>";
  }

  /**
   * This is executed on non-indexed fields.
   */
  @Override
  public Object evaluateRecord(
      final YTIdentifiable iRecord,
      YTEntityImpl iCurrentResult,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {
    if (iLeft == null || iRight == null) {
      return false;
    }

    return iLeft.toString().indexOf(iRight.toString()) > -1;
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.INDEX_METHOD;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    return null;
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
