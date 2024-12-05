/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.core.sql.functions;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionDifference;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionDistinct;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionDocument;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionFirst;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionIntersect;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionLast;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionList;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionMap;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionSet;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionSymmetricDifference;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionTraversedEdge;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionTraversedElement;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionTraversedVertex;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLFunctionUnionAll;
import com.jetbrains.youtrack.db.internal.core.sql.functions.geo.OSQLFunctionDistance;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionAstar;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionBoth;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionBothE;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionBothV;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionDijkstra;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionIn;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionInE;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionInV;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionOut;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionOutE;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionOutV;
import com.jetbrains.youtrack.db.internal.core.sql.functions.graph.OSQLFunctionShortestPath;
import com.jetbrains.youtrack.db.internal.core.sql.functions.math.OSQLFunctionAbsoluteValue;
import com.jetbrains.youtrack.db.internal.core.sql.functions.math.OSQLFunctionAverage;
import com.jetbrains.youtrack.db.internal.core.sql.functions.math.OSQLFunctionDecimal;
import com.jetbrains.youtrack.db.internal.core.sql.functions.math.OSQLFunctionEval;
import com.jetbrains.youtrack.db.internal.core.sql.functions.math.OSQLFunctionInterval;
import com.jetbrains.youtrack.db.internal.core.sql.functions.math.OSQLFunctionMax;
import com.jetbrains.youtrack.db.internal.core.sql.functions.math.OSQLFunctionMin;
import com.jetbrains.youtrack.db.internal.core.sql.functions.math.OSQLFunctionSum;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionCoalesce;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionCount;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionDate;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionDecode;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionEncode;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionIf;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionIfNull;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionIndexKeySize;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionStrcmpci;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionSysdate;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionThrowCME;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLFunctionUUID;
import com.jetbrains.youtrack.db.internal.core.sql.functions.sequence.OSQLFunctionSequence;
import com.jetbrains.youtrack.db.internal.core.sql.functions.stat.OSQLFunctionMedian;
import com.jetbrains.youtrack.db.internal.core.sql.functions.stat.OSQLFunctionMode;
import com.jetbrains.youtrack.db.internal.core.sql.functions.stat.OSQLFunctionPercentile;
import com.jetbrains.youtrack.db.internal.core.sql.functions.stat.OSQLFunctionStandardDeviation;
import com.jetbrains.youtrack.db.internal.core.sql.functions.stat.OSQLFunctionVariance;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLFunctionConcat;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLFunctionFormat;

/**
 * Default set of SQL function.
 */
public final class ODefaultSQLFunctionFactory extends OSQLFunctionFactoryTemplate {

  @Override
  public void registerDefaultFunctions(YTDatabaseSessionInternal db) {
    // MISC FUNCTIONS
    register(OSQLFunctionAverage.NAME, OSQLFunctionAverage.class);
    register(OSQLFunctionCoalesce.NAME, new OSQLFunctionCoalesce());
    register(OSQLFunctionCount.NAME, OSQLFunctionCount.class);
    register(OSQLFunctionDate.NAME, OSQLFunctionDate.class);
    register(OSQLFunctionDecode.NAME, new OSQLFunctionDecode());
    register(OSQLFunctionDifference.NAME, OSQLFunctionDifference.class);
    register(OSQLFunctionSymmetricDifference.NAME, OSQLFunctionSymmetricDifference.class);
    register(OSQLFunctionDistance.NAME, new OSQLFunctionDistance());
    register(OSQLFunctionDistinct.NAME, OSQLFunctionDistinct.class);
    register(OSQLFunctionDocument.NAME, OSQLFunctionDocument.class);
    register(OSQLFunctionEncode.NAME, new OSQLFunctionEncode());
    register(OSQLFunctionEval.NAME, OSQLFunctionEval.class);
    register(OSQLFunctionFirst.NAME, new OSQLFunctionFirst());
    register(OSQLFunctionFormat.NAME, new OSQLFunctionFormat());
    register(OSQLFunctionTraversedEdge.NAME, OSQLFunctionTraversedEdge.class);
    register(OSQLFunctionTraversedElement.NAME, OSQLFunctionTraversedElement.class);
    register(OSQLFunctionTraversedVertex.NAME, OSQLFunctionTraversedVertex.class);
    register(OSQLFunctionIf.NAME, new OSQLFunctionIf());
    register(OSQLFunctionIfNull.NAME, new OSQLFunctionIfNull());
    register(OSQLFunctionIntersect.NAME, OSQLFunctionIntersect.class);
    register(OSQLFunctionLast.NAME, new OSQLFunctionLast());
    register(OSQLFunctionList.NAME, OSQLFunctionList.class);
    register(OSQLFunctionMap.NAME, OSQLFunctionMap.class);
    register(OSQLFunctionMax.NAME, OSQLFunctionMax.class);
    register(OSQLFunctionMin.NAME, OSQLFunctionMin.class);
    register(OSQLFunctionInterval.NAME, OSQLFunctionInterval.class);
    register(OSQLFunctionSet.NAME, OSQLFunctionSet.class);
    register(OSQLFunctionSysdate.NAME, OSQLFunctionSysdate.class);
    register(OSQLFunctionSum.NAME, OSQLFunctionSum.class);
    register(OSQLFunctionUnionAll.NAME, OSQLFunctionUnionAll.class);
    register(OSQLFunctionMode.NAME, OSQLFunctionMode.class);
    register(OSQLFunctionPercentile.NAME, OSQLFunctionPercentile.class);
    register(OSQLFunctionMedian.NAME, OSQLFunctionMedian.class);
    register(OSQLFunctionVariance.NAME, OSQLFunctionVariance.class);
    register(OSQLFunctionStandardDeviation.NAME, OSQLFunctionStandardDeviation.class);
    register(OSQLFunctionUUID.NAME, OSQLFunctionUUID.class);
    register(OSQLFunctionConcat.NAME, OSQLFunctionConcat.class);
    register(OSQLFunctionDecimal.NAME, OSQLFunctionDecimal.class);
    register(OSQLFunctionSequence.NAME, OSQLFunctionSequence.class);
    register(OSQLFunctionAbsoluteValue.NAME, OSQLFunctionAbsoluteValue.class);
    register(OSQLFunctionIndexKeySize.NAME, OSQLFunctionIndexKeySize.class);
    register(OSQLFunctionStrcmpci.NAME, OSQLFunctionStrcmpci.class);
    register(OSQLFunctionThrowCME.NAME, OSQLFunctionThrowCME.class);
    // graph
    register(OSQLFunctionOut.NAME, OSQLFunctionOut.class);
    register(OSQLFunctionIn.NAME, OSQLFunctionIn.class);
    register(OSQLFunctionBoth.NAME, OSQLFunctionBoth.class);
    register(OSQLFunctionOutE.NAME, OSQLFunctionOutE.class);
    register(OSQLFunctionOutV.NAME, OSQLFunctionOutV.class);
    register(OSQLFunctionInE.NAME, OSQLFunctionInE.class);
    register(OSQLFunctionInV.NAME, OSQLFunctionInV.class);
    register(OSQLFunctionBothE.NAME, OSQLFunctionBothE.class);
    register(OSQLFunctionBothV.NAME, OSQLFunctionBothV.class);
    register(OSQLFunctionShortestPath.NAME, OSQLFunctionShortestPath.class);
    register(OSQLFunctionDijkstra.NAME, OSQLFunctionDijkstra.class);
    register(OSQLFunctionAstar.NAME, OSQLFunctionAstar.class);
  }
}
