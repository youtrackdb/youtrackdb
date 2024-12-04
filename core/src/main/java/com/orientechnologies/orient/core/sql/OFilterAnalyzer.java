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

package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorBetween;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContains;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinorEquals;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class OFilterAnalyzer {

  public static List<OIndex> getInvolvedIndexes(
      YTDatabaseSessionInternal session,
      YTClass iSchemaClass,
      OIndexSearchResult searchResultFields) {
    final Set<OIndex> involvedIndexes =
        iSchemaClass.getInvolvedIndexes(session, searchResultFields.fields());

    final List<OIndex> result = new ArrayList<OIndex>(involvedIndexes.size());

    if (searchResultFields.lastField.isLong()) {
      result.addAll(
          OChainedIndexProxy.createProxies(session, iSchemaClass, searchResultFields.lastField));
    } else {
      result.addAll(involvedIndexes);
    }

    return result;
  }

  public List<List<OIndexSearchResult>> analyzeMainCondition(
      OSQLFilterCondition condition, final YTClass schemaClass, OCommandContext context) {
    return analyzeOrFilterBranch(schemaClass, condition, context);
  }

  private List<List<OIndexSearchResult>> analyzeOrFilterBranch(
      final YTClass iSchemaClass, OSQLFilterCondition condition, OCommandContext iContext) {
    if (condition == null) {
      return null;
    }

    OQueryOperator operator = condition.getOperator();

    while (operator == null) {
      if (condition.getRight() == null && condition.getLeft() instanceof OSQLFilterCondition) {
        condition = (OSQLFilterCondition) condition.getLeft();
        operator = condition.getOperator();
      } else {
        return null;
      }
    }

    final OIndexReuseType indexReuseType =
        operator.getIndexReuseType(condition.getLeft(), condition.getRight());
    if (OIndexReuseType.INDEX_UNION.equals(indexReuseType)) {
      return analyzeUnion(iSchemaClass, condition, iContext);
    }

    List<List<OIndexSearchResult>> result = new ArrayList<List<OIndexSearchResult>>();
    List<OIndexSearchResult> sub = analyzeCondition(condition, iSchemaClass, iContext);
    //    analyzeFilterBranch(iSchemaClass, condition, sub, iContext);
    result.add(sub);
    return result;
  }

  /**
   * Analyzes a query filter for a possible indexation options. The results are sorted by amount of
   * fields. So the most specific items go first.
   *
   * @param condition   to analyze
   * @param schemaClass the class that is scanned by query
   * @param context     of the query
   * @return list of OIndexSearchResult items
   */
  public List<OIndexSearchResult> analyzeCondition(
      OSQLFilterCondition condition, final YTClass schemaClass, OCommandContext context) {

    final List<OIndexSearchResult> indexSearchResults = new ArrayList<OIndexSearchResult>();
    OIndexSearchResult lastCondition =
        analyzeFilterBranch(context.getDatabase(), schemaClass, condition, indexSearchResults,
            context);

    if (indexSearchResults.isEmpty() && lastCondition != null) {
      indexSearchResults.add(lastCondition);
    }
    indexSearchResults.sort((searchResultOne, searchResultTwo) ->
        searchResultTwo.getFieldCount() - searchResultOne.getFieldCount());

    return indexSearchResults;
  }

  private OIndexSearchResult analyzeFilterBranch(
      YTDatabaseSession session, final YTClass iSchemaClass,
      OSQLFilterCondition condition,
      final List<OIndexSearchResult> iIndexSearchResults,
      OCommandContext iContext) {
    if (condition == null) {
      return null;
    }

    OQueryOperator operator = condition.getOperator();

    while (operator == null) {
      if (condition.getRight() == null && condition.getLeft() instanceof OSQLFilterCondition) {
        condition = (OSQLFilterCondition) condition.getLeft();
        operator = condition.getOperator();
      } else {
        return null;
      }
    }

    final OIndexReuseType indexReuseType =
        operator.getIndexReuseType(condition.getLeft(), condition.getRight());
    return switch (indexReuseType) {
      case INDEX_INTERSECTION ->
          analyzeIntersection(session, iSchemaClass, condition, iIndexSearchResults, iContext);
      case INDEX_METHOD ->
          analyzeIndexMethod(session, iSchemaClass, condition, iIndexSearchResults, iContext);
      case INDEX_OPERATOR ->
          analyzeOperator(iSchemaClass, condition, iIndexSearchResults, iContext);
      default -> null;
    };
  }

  private static OIndexSearchResult analyzeOperator(
      YTClass iSchemaClass,
      OSQLFilterCondition condition,
      List<OIndexSearchResult> iIndexSearchResults,
      OCommandContext iContext) {
    return condition
        .getOperator()
        .getOIndexSearchResult(iSchemaClass, condition, iIndexSearchResults, iContext);
  }

  private static OIndexSearchResult analyzeIndexMethod(
      YTDatabaseSession session, YTClass iSchemaClass,
      OSQLFilterCondition condition,
      List<OIndexSearchResult> iIndexSearchResults,
      OCommandContext ctx) {
    OIndexSearchResult result = createIndexedProperty(condition, condition.getLeft(), ctx);
    if (result == null) {
      result = createIndexedProperty(condition, condition.getRight(), ctx);
    }

    if (result == null) {
      return null;
    }

    if (checkIndexExistence(session, iSchemaClass, result)) {
      iIndexSearchResults.add(result);
    }

    return result;
  }

  private OIndexSearchResult analyzeIntersection(
      YTDatabaseSession session, YTClass iSchemaClass,
      OSQLFilterCondition condition,
      List<OIndexSearchResult> iIndexSearchResults,
      OCommandContext iContext) {
    final OIndexSearchResult leftResult =
        analyzeFilterBranch(session
            , iSchemaClass, (OSQLFilterCondition) condition.getLeft(), iIndexSearchResults,
            iContext);
    final OIndexSearchResult rightResult =
        analyzeFilterBranch(session
            , iSchemaClass,
            (OSQLFilterCondition) condition.getRight(),
            iIndexSearchResults, iContext);

    if (leftResult != null && rightResult != null) {
      if (leftResult.canBeMerged(rightResult)) {
        final OIndexSearchResult mergeResult = leftResult.merge(rightResult);
        if (iSchemaClass.areIndexed(iContext.getDatabase(), mergeResult.fields())) {
          iIndexSearchResults.add(mergeResult);
        }

        return leftResult.merge(rightResult);
      }
    }

    return null;
  }

  private List<List<OIndexSearchResult>> analyzeUnion(
      YTClass iSchemaClass, OSQLFilterCondition condition, OCommandContext iContext) {
    List<List<OIndexSearchResult>> result = new ArrayList<List<OIndexSearchResult>>();

    result.addAll(
        analyzeOrFilterBranch(iSchemaClass, (OSQLFilterCondition) condition.getLeft(), iContext));
    result.addAll(
        analyzeOrFilterBranch(iSchemaClass, (OSQLFilterCondition) condition.getRight(), iContext));

    return result;
  }

  /**
   * Add SQL filter field to the search candidate list.
   *
   * @param iCondition Condition item
   * @param iItem      Value to search
   * @return true if the property was indexed and found, otherwise false
   */
  private static OIndexSearchResult createIndexedProperty(
      final OSQLFilterCondition iCondition, final Object iItem, OCommandContext ctx) {
    if (iItem == null || !(iItem instanceof OSQLFilterItemField item)) {
      return null;
    }

    if (iCondition.getLeft() instanceof OSQLFilterItemField
        && iCondition.getRight() instanceof OSQLFilterItemField) {
      return null;
    }

    if (item.hasChainOperators() && !item.isFieldChain()) {
      return null;
    }

    boolean inverted = iCondition.getRight() == iItem;
    final Object origValue = inverted ? iCondition.getLeft() : iCondition.getRight();

    OQueryOperator operator = iCondition.getOperator();

    if (inverted) {
      if (operator instanceof OQueryOperatorIn) {
        operator = new OQueryOperatorContains();
      } else if (operator instanceof OQueryOperatorContains) {
        operator = new OQueryOperatorIn();
      } else if (operator instanceof OQueryOperatorMajor) {
        operator = new OQueryOperatorMinor();
      } else if (operator instanceof OQueryOperatorMinor) {
        operator = new OQueryOperatorMajor();
      } else if (operator instanceof OQueryOperatorMajorEquals) {
        operator = new OQueryOperatorMinorEquals();
      } else if (operator instanceof OQueryOperatorMinorEquals) {
        operator = new OQueryOperatorMajorEquals();
      }
    }

    if (iCondition.getOperator() instanceof OQueryOperatorBetween
        || operator instanceof OQueryOperatorIn) {

      return new OIndexSearchResult(operator, item.getFieldChain(), origValue);
    }

    final Object value = OSQLHelper.getValue(origValue, null, ctx);
    return new OIndexSearchResult(operator, item.getFieldChain(), value);
  }

  private static boolean checkIndexExistence(YTDatabaseSession session, final YTClass iSchemaClass,
      final OIndexSearchResult result) {
    return iSchemaClass.areIndexed(session, result.fields())
        && (!result.lastField.isLong() || checkIndexChainExistence(session, iSchemaClass, result));
  }

  private static boolean checkIndexChainExistence(YTDatabaseSession session, YTClass iSchemaClass,
      OIndexSearchResult result) {
    final int fieldCount = result.lastField.getItemCount();
    YTClass cls = iSchemaClass.getProperty(result.lastField.getItemName(0)).getLinkedClass();

    for (int i = 1; i < fieldCount; i++) {
      if (cls == null || !cls.areIndexed(session, result.lastField.getItemName(i))) {
        return false;
      }

      cls = cls.getProperty(result.lastField.getItemName(i)).getLinkedClass();
    }
    return true;
  }
}
