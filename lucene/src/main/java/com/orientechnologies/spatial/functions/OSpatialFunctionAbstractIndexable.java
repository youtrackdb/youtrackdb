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
package com.orientechnologies.spatial.functions;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLLtOperator;
import com.orientechnologies.lucene.collections.OLuceneResultSetEmpty;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadataInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OIndexableSQLFunction;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBinaryCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLJson;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLLeOperator;
import com.orientechnologies.spatial.index.OLuceneSpatialIndex;
import com.orientechnologies.spatial.strategy.SpatialQueryBuilderAbstract;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class OSpatialFunctionAbstractIndexable extends OSpatialFunctionAbstract
    implements OIndexableSQLFunction {

  public OSpatialFunctionAbstractIndexable(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  protected OLuceneSpatialIndex searchForIndex(YTDatabaseSessionInternal session,
      SQLFromClause target,
      SQLExpression[] args) {
    OMetadataInternal dbMetadata = getDb().getMetadata();

    SQLFromItem item = target.getItem();
    SQLIdentifier identifier = item.getIdentifier();
    String fieldName = args[0].toString();

    String className = identifier.getStringValue();
    List<OLuceneSpatialIndex> indices =
        dbMetadata.getImmutableSchemaSnapshot().getClass(className).getIndexes(session).stream()
            .filter(idx -> idx instanceof OLuceneSpatialIndex)
            .map(idx -> (OLuceneSpatialIndex) idx)
            .filter(
                idx ->
                    intersect(
                        idx.getDefinition().getFields(), Collections.singletonList(fieldName)))
            .toList();

    if (indices.size() > 1) {
      throw new IllegalArgumentException(
          "too many indices matching given field name: " + String.join(",", fieldName));
    }

    return indices.isEmpty() ? null : indices.get(0);
  }

  protected YTDatabaseSessionInternal getDb() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  protected Iterable<YTIdentifiable> results(
      SQLFromClause target, SQLExpression[] args, CommandContext ctx, Object rightValue) {
    OIndex oIndex = searchForIndex(ctx.getDatabase(), target, args);

    if (oIndex == null) {
      return null;
    }

    Map<String, Object> queryParams = new HashMap<>();
    queryParams.put(SpatialQueryBuilderAbstract.GEO_FILTER, operator());
    Object shape;
    if (args[1].getValue() instanceof SQLJson json) {
      EntityImpl doc = new EntityImpl();
      doc.fromJSON(json.toString());
      shape = doc.toMap();
    } else {
      shape = args[1].execute((YTIdentifiable) null, ctx);
    }

    if (shape instanceof Collection) {
      int size = ((Collection) shape).size();

      if (size == 0) {
        return new OLuceneResultSetEmpty();
      }
      if (size == 1) {

        Object next = ((Collection) shape).iterator().next();

        if (next instanceof YTResult inner) {
          var propertyNames = inner.getPropertyNames();
          if (propertyNames.size() == 1) {
            Object property = inner.getProperty(propertyNames.iterator().next());
            if (property instanceof YTResult) {
              shape = ((YTResult) property).toEntity();
            }
          } else {
            return new OLuceneResultSetEmpty();
          }
        }
      } else {
        throw new YTCommandExecutionException("The collection in input cannot be major than 1");
      }
    }

    if (shape instanceof YTResultInternal) {
      shape = ((YTResultInternal) shape).toEntity();
    }
    queryParams.put(SpatialQueryBuilderAbstract.SHAPE, shape);

    onAfterParsing(queryParams, args, ctx, rightValue);

    Set<String> indexes = (Set<String>) ctx.getVariable("involvedIndexes");
    if (indexes == null) {
      indexes = new HashSet<>();
      ctx.setVariable("involvedIndexes", indexes);
    }
    indexes.add(oIndex.getName());
    return oIndex.getInternal().getRids(ctx.getDatabase(), queryParams).collect(Collectors.toSet());
  }

  protected void onAfterParsing(
      Map<String, Object> params, SQLExpression[] args, CommandContext ctx, Object rightValue) {
  }

  protected abstract String operator();

  @Override
  public boolean canExecuteInline(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {

    return true;
  }

  @Override
  public boolean allowsIndexedExecution(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {

    if (!isValidBinaryOperator(operator)) {
      return false;
    }
    OLuceneSpatialIndex index = searchForIndex(ctx.getDatabase(), target, args);

    return index != null;
  }

  @Override
  public boolean shouldExecuteAfterSearch(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {
    return true;
  }

  @Override
  public long estimate(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {

    OLuceneSpatialIndex index = searchForIndex(ctx.getDatabase(), target, args);
    return index == null ? -1 : index.size(ctx.getDatabase());
  }

  public static <T> boolean intersect(List<T> list1, List<T> list2) {
    for (T t : list1) {
      if (list2.contains(t)) {
        return true;
      }
    }

    return false;
  }

  protected boolean isValidBinaryOperator(SQLBinaryCompareOperator operator) {
    return operator instanceof SQLLtOperator || operator instanceof SQLLeOperator;
  }
}
