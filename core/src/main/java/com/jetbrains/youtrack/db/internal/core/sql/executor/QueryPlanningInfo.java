package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLAndBlock;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLGroupBy;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLLetClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLLimit;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLProjection;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLSkip;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLTimeout;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLUnwind;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLWhereClause;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class QueryPlanningInfo {

  protected SQLTimeout timeout;
  protected boolean distinct = false;
  protected boolean expand = false;

  protected SQLProjection preAggregateProjection;
  protected SQLProjection aggregateProjection;
  protected SQLProjection projection = null;
  protected SQLProjection projectionAfterOrderBy = null;

  protected SQLLetClause globalLetClause = null;
  protected boolean globalLetPresent = false;

  protected SQLLetClause perRecordLetClause = null;

  /**
   * in a sharded execution plan, this maps the single server to the clusters it will be queried for
   * to execute the query.
   */
  protected Map<String, Set<String>> serverToClusters;

  protected Map<String, SelectExecutionPlan> distributedFetchExecutionPlans;

  /**
   * set to true when the distributedFetchExecutionPlans are aggregated in the main execution plan
   */
  public boolean distributedPlanCreated = false;

  protected SQLFromClause target;
  protected SQLWhereClause whereClause;
  protected List<SQLAndBlock> flattenedWhereClause;
  protected SQLGroupBy groupBy;
  protected SQLOrderBy orderBy;
  protected SQLUnwind unwind;
  protected SQLSkip skip;
  protected SQLLimit limit;

  protected boolean orderApplied = false;
  protected boolean projectionsCalculated = false;

  protected SQLAndBlock ridRangeConditions;

  public QueryPlanningInfo copy() {
    // TODO check what has to be copied and what can be just referenced as it is
    QueryPlanningInfo result = new QueryPlanningInfo();
    result.distinct = this.distinct;
    result.expand = this.expand;
    result.preAggregateProjection = this.preAggregateProjection;
    result.aggregateProjection = this.aggregateProjection;
    result.projection = this.projection;
    result.projectionAfterOrderBy = this.projectionAfterOrderBy;
    result.globalLetClause = this.globalLetClause;
    result.globalLetPresent = this.globalLetPresent;
    result.perRecordLetClause = this.perRecordLetClause;
    result.serverToClusters = this.serverToClusters;

    //    Map<String, SelectExecutionPlan> distributedFetchExecutionPlans;//TODO!

    result.distributedPlanCreated = this.distributedPlanCreated;
    result.target = this.target;
    result.whereClause = this.whereClause;
    result.flattenedWhereClause = this.flattenedWhereClause;
    result.groupBy = this.groupBy;
    result.orderBy = this.orderBy;
    result.unwind = this.unwind;
    result.skip = this.skip;
    result.limit = this.limit;
    result.orderApplied = this.orderApplied;
    result.projectionsCalculated = this.projectionsCalculated;
    result.ridRangeConditions = this.ridRangeConditions;

    return result;
  }
}
