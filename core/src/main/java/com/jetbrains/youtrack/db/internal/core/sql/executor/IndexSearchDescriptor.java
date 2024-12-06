package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.index.CompositeIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLAndBlock;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBinaryCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBinaryCondition;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBooleanExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLEqualsCompareOperator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class IndexSearchDescriptor {

  private final Index index;
  private final SQLBooleanExpression keyCondition;
  private final SQLBinaryCondition additionalRangeCondition;
  private final SQLBooleanExpression remainingCondition;

  public IndexSearchDescriptor(
      Index idx,
      SQLBooleanExpression keyCondition,
      SQLBinaryCondition additional,
      SQLBooleanExpression remainingCondition) {
    this.index = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = additional;
    this.remainingCondition = remainingCondition;
  }

  public IndexSearchDescriptor(Index idx) {
    this.index = idx;
    this.keyCondition = null;
    this.additionalRangeCondition = null;
    this.remainingCondition = null;
  }

  public IndexSearchDescriptor(Index idx, SQLBooleanExpression keyCondition) {
    this.index = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = null;
    this.remainingCondition = null;
  }

  public int cost(CommandContext ctx) {
    QueryStats stats = QueryStats.get(ctx.getDatabase());

    String indexName = index.getName();
    int size = getSubBlocks().size();
    boolean range = false;
    SQLBooleanExpression lastOp = getSubBlocks().get(getSubBlocks().size() - 1);
    if (lastOp instanceof SQLBinaryCondition) {
      SQLBinaryCompareOperator op = ((SQLBinaryCondition) lastOp).getOperator();
      range = op.isRangeOperator();
    }

    long val =
        stats.getIndexStats(
            indexName, size, range, additionalRangeCondition != null, ctx.getDatabase());
    if (val == -1) {
      // TODO query the index!
    }
    if (val >= 0) {
      return val > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) val;
    }
    return Integer.MAX_VALUE;
  }

  private List<SQLBooleanExpression> getSubBlocks() {
    if (keyCondition instanceof SQLAndBlock) {
      return ((SQLAndBlock) keyCondition).getSubBlocks();
    } else {
      return Collections.singletonList(keyCondition);
    }
  }

  public int blockCount() {
    return getSubBlocks().size();
  }

  protected Index getIndex() {
    return index;
  }

  protected SQLBooleanExpression getKeyCondition() {
    return keyCondition;
  }

  protected SQLBinaryCondition getAdditionalRangeCondition() {
    return additionalRangeCondition;
  }

  protected SQLBooleanExpression getRemainingCondition() {
    return remainingCondition;
  }

  /**
   * checks whether the condition has CONTAINSANY or similar expressions, that require multiple
   * index evaluations
   *
   * @param keyCondition
   * @return
   */
  public boolean requiresMultipleIndexLookups() {
    for (SQLBooleanExpression oBooleanExpression : getSubBlocks()) {
      if (!(oBooleanExpression instanceof SQLBinaryCondition)) {
        return true;
      }
    }
    return false;
  }

  public boolean requiresDistinctStep() {
    return requiresMultipleIndexLookups() || duplicateResultsForRecord();
  }

  public boolean duplicateResultsForRecord() {
    if (index.getDefinition() instanceof CompositeIndexDefinition) {
      return ((CompositeIndexDefinition) index.getDefinition()).getMultiValueDefinition() != null;
    }
    return false;
  }

  public boolean fullySorted(List<String> orderItems) {
    List<SQLBooleanExpression> conditions = getSubBlocks();
    Index idx = index;

    if (!idx.supportsOrderedIterations()) {
      return false;
    }
    List<String> conditionItems = new ArrayList<>();

    for (int i = 0; i < conditions.size(); i++) {
      SQLBooleanExpression item = conditions.get(i);
      if (item instanceof SQLBinaryCondition) {
        if (((SQLBinaryCondition) item).getOperator() instanceof SQLEqualsCompareOperator) {
          conditionItems.add(((SQLBinaryCondition) item).getLeft().toString());
        } else if (i != conditions.size() - 1) {
          return false;
        }

      } else if (i != conditions.size() - 1) {
        return false;
      }
    }

    List<String> orderedFields = new ArrayList<>();
    boolean overlapping = false;
    for (String s : conditionItems) {
      if (orderItems.isEmpty()) {
        return true; // nothing to sort, the conditions completely overlap the ORDER BY
      }
      if (s.equals(orderItems.get(0))) {
        orderItems.remove(0);
        overlapping = true; // start overlapping
      } else if (overlapping) {
        return false; // overlapping, but next order item does not match...
      }
      orderedFields.add(s);
    }
    orderedFields.addAll(orderItems);

    final IndexDefinition definition = idx.getDefinition();
    final List<String> fields = definition.getFields();
    if (fields.size() < orderedFields.size()) {
      return false;
    }

    for (int i = 0; i < orderedFields.size(); i++) {
      final String orderFieldName = orderedFields.get(i);
      final String indexFieldName = fields.get(i);
      if (!orderFieldName.equals(indexFieldName)) {
        return false;
      }
    }

    return true;
  }

  /**
   * returns true if the first argument is a prefix for the second argument, eg. if the first
   * argument is [a] and the second argument is [a, b]
   *
   * @param item
   * @param desc
   * @return
   */
  public boolean isPrefixOf(IndexSearchDescriptor other) {
    List<SQLBooleanExpression> left = getSubBlocks();
    List<SQLBooleanExpression> right = other.getSubBlocks();
    if (left.size() > right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); i++) {
      if (!left.get(i).equals(right.get(i))) {
        return false;
      }
    }
    return true;
  }

  public boolean isSameCondition(IndexSearchDescriptor desc) {
    if (blockCount() != desc.blockCount()) {
      return false;
    }
    List<SQLBooleanExpression> left = getSubBlocks();
    List<SQLBooleanExpression> right = desc.getSubBlocks();
    for (int i = 0; i < left.size(); i++) {
      if (!left.get(i).equals(right.get(i))) {
        return false;
      }
    }
    return true;
  }
}
