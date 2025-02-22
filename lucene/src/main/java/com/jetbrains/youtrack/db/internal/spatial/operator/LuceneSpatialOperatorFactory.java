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
package com.jetbrains.youtrack.db.internal.spatial.operator;

import com.jetbrains.youtrack.db.internal.core.sql.operator.QueryOperator;
import com.jetbrains.youtrack.db.internal.core.sql.operator.QueryOperatorFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class LuceneSpatialOperatorFactory implements QueryOperatorFactory {

  public static final Set<QueryOperator> OPERATORS;

  static {
    final Set<QueryOperator> operators =
        new HashSet<QueryOperator>() {
          {
            add(new LuceneNearOperator());
            add(new LuceneWithinOperator());
            add(new LuceneOverlapOperator());
          }
        };

    OPERATORS = Collections.unmodifiableSet(operators);
  }

  @Override
  public Set<QueryOperator> getOperators() {
    return OPERATORS;
  }
}
