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
package com.jetbrains.youtrack.db.internal.core.sql.operator;

import com.jetbrains.youtrack.db.internal.common.profiler.OProfiler;
import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.OIndexSearchResult;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;
import com.jetbrains.youtrack.db.internal.core.sql.operator.math.OQueryOperatorDivide;
import com.jetbrains.youtrack.db.internal.core.sql.operator.math.OQueryOperatorMinus;
import com.jetbrains.youtrack.db.internal.core.sql.operator.math.OQueryOperatorMod;
import com.jetbrains.youtrack.db.internal.core.sql.operator.math.OQueryOperatorMultiply;
import com.jetbrains.youtrack.db.internal.core.sql.operator.math.OQueryOperatorPlus;
import java.util.List;
import java.util.stream.Stream;

/**
 * Query Operators. Remember to handle the operator in OQueryItemCondition.
 */
public abstract class OQueryOperator {

  public enum ORDER {
    /**
     * Used when order compared to other operator cannot be evaluated or has no consequences.
     */
    UNKNOWNED,
    /**
     * Used when this operator must be before the other one
     */
    BEFORE,
    /**
     * Used when this operator must be after the other one
     */
    AFTER,
    /**
     * Used when this operator is equal the other one
     */
    EQUAL
  }

  /**
   * Default operator order. can be used by additional operator to locate themself relatively to
   * default ones.
   *
   * <p>WARNING: ORDER IS IMPORTANT TO AVOID SUB-STRING LIKE "IS" and AND "INSTANCEOF": INSTANCEOF
   * MUST BE PLACED BEFORE! AND ALSO FOR PERFORMANCE (MOST USED BEFORE)
   */
  protected static final Class<?>[] DEFAULT_OPERATORS_ORDER = {
      OQueryOperatorEquals.class,
      OQueryOperatorAnd.class,
      OQueryOperatorOr.class,
      OQueryOperatorNotEquals.class,
      OQueryOperatorNotEquals2.class,
      OQueryOperatorNot.class,
      OQueryOperatorMinorEquals.class,
      OQueryOperatorMinor.class,
      OQueryOperatorMajorEquals.class,
      OQueryOperatorContainsAll.class,
      OQueryOperatorMajor.class,
      OQueryOperatorLike.class,
      OQueryOperatorMatches.class,
      OQueryOperatorInstanceof.class,
      OQueryOperatorIs.class,
      OQueryOperatorIn.class,
      OQueryOperatorContainsKey.class,
      OQueryOperatorContainsValue.class,
      OQueryOperatorContainsText.class,
      OQueryOperatorContains.class,
      OQueryOperatorTraverse.class,
      OQueryOperatorBetween.class,
      OQueryOperatorPlus.class,
      OQueryOperatorMinus.class,
      OQueryOperatorMultiply.class,
      OQueryOperatorDivide.class,
      OQueryOperatorMod.class
  };

  public final String keyword;
  public final int precedence;
  public final int expectedRightWords;
  public final boolean unary;
  public final boolean expectsParameters;

  protected OQueryOperator(final String iKeyword, final int iPrecedence, final boolean iUnary) {
    this(iKeyword, iPrecedence, iUnary, 1, false);
  }

  protected OQueryOperator(
      final String iKeyword,
      final int iPrecedence,
      final boolean iUnary,
      final int iExpectedRightWords) {
    this(iKeyword, iPrecedence, iUnary, iExpectedRightWords, false);
  }

  protected OQueryOperator(
      final String iKeyword,
      final int iPrecedence,
      final boolean iUnary,
      final int iExpectedRightWords,
      final boolean iExpectsParameters) {
    keyword = iKeyword;
    precedence = iPrecedence;
    unary = iUnary;
    expectedRightWords = iExpectedRightWords;
    expectsParameters = iExpectsParameters;
  }

  public abstract Object evaluateRecord(
      final YTIdentifiable iRecord,
      EntityImpl iCurrentResult,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      CommandContext iContext,
      final ODocumentSerializer serializer);

  /**
   * Returns hint how index can be used to calculate result of operator execution.
   *
   * @param iLeft  Value of left query parameter.
   * @param iRight Value of right query parameter.
   * @return Hint how index can be used to calculate result of operator execution.
   */
  public abstract OIndexReuseType getIndexReuseType(Object iLeft, Object iRight);

  public OIndexSearchResult getOIndexSearchResult(
      YTClass iSchemaClass,
      OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults,
      CommandContext context) {

    return null;
  }

  /**
   * Performs index query and returns index stream which presents subset of index data which
   * corresponds to result of execution of given operator.
   *
   * <p>Query that should be executed can be presented like: [[property0 = keyParam0] and
   * [property1 = keyParam1] and] propertyN operator keyParamN.
   *
   * <p>It is supped that index which passed in as parameter is used to index properties listed
   * above and responsibility of given method execute query using given parameters.
   *
   * <p>Multiple parameters are passed in to implement composite indexes support.
   *
   * @param iContext
   * @param index        Instance of index that will be used to calculate result of operator
   *                     execution.
   * @param keyParams    Parameters of query is used to calculate query result.
   * @param ascSortOrder Data returned by cursors should be sorted in ascending or descending
   *                     order.
   * @return Cursor instance if index can be used to evaluate result of execution of given operator
   * and <code>null</code> otherwise.
   */
  public Stream<ORawPair<Object, YTRID>> executeIndexQuery(
      CommandContext iContext, OIndex index, final List<Object> keyParams, boolean ascSortOrder) {
    return Stream.empty();
  }

  @Override
  public String toString() {
    return keyword;
  }

  /**
   * Default State-less implementation: does not save parameters and just return itself
   *
   * @param iParams
   * @return
   */
  public OQueryOperator configure(final List<String> iParams) {
    return this;
  }

  public String getSyntax() {
    return "<left> " + keyword + " <right>";
  }

  public abstract YTRID getBeginRidRange(YTDatabaseSession session, final Object iLeft,
      final Object iRight);

  public abstract YTRID getEndRidRange(YTDatabaseSession session, final Object iLeft,
      final Object iRight);

  public boolean isUnary() {
    return unary;
  }

  /**
   * Check priority of this operator compare to given operator.
   *
   * @param other
   * @return ORDER place of this operator compared to given operator
   */
  public ORDER compare(OQueryOperator other) {
    final Class<?> thisClass = this.getClass();
    final Class<?> otherClass = other.getClass();

    int thisPosition = -1;
    int otherPosition = -1;
    for (int i = 0; i < DEFAULT_OPERATORS_ORDER.length; i++) {
      // subclass of default operators inherit their parent ordering
      final Class<?> clazz = DEFAULT_OPERATORS_ORDER[i];
      if (clazz.isAssignableFrom(thisClass)) {
        thisPosition = i;
      }
      if (clazz.isAssignableFrom(otherClass)) {
        otherPosition = i;
      }
    }

    if (thisPosition == -1 || otherPosition == -1) {
      // cannot decide which comes first
      return ORDER.UNKNOWNED;
    }

    if (thisPosition > otherPosition) {
      return ORDER.AFTER;
    } else if (thisPosition < otherPosition) {
      return ORDER.BEFORE;
    }

    return ORDER.EQUAL;
  }

  protected void updateProfiler(
      final CommandContext iContext,
      final OIndex index,
      final List<Object> keyParams,
      final OIndexDefinition indexDefinition) {
    if (iContext.isRecordingMetrics()) {
      iContext.updateMetric("compositeIndexUsed", +1);
    }

    final OProfiler profiler = YouTrackDBManager.instance().getProfiler();
    if (profiler.isRecording()) {
      profiler.updateCounter(
          profiler.getDatabaseMetric(index.getDatabaseName(), "query.indexUsed"),
          "Used index in query",
          +1);

      int params = indexDefinition.getParamCount();
      if (params > 1) {
        final String profiler_prefix =
            profiler.getDatabaseMetric(index.getDatabaseName(), "query.compositeIndexUsed");

        profiler.updateCounter(profiler_prefix, "Used composite index in query", +1);
        profiler.updateCounter(
            profiler_prefix + "." + params,
            "Used composite index in query with " + params + " params",
            +1);
        profiler.updateCounter(
            profiler_prefix + "." + params + '.' + keyParams.size(),
            "Used composite index in query with "
                + params
                + " params and "
                + keyParams.size()
                + " keys",
            +1);
      }
    }
  }

  public boolean canShortCircuit(Object l) {
    return false;
  }

  public boolean canBeMerged() {
    return true;
  }

  public boolean isSupportingBinaryEvaluate() {
    return false;
  }
}
