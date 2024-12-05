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
package com.jetbrains.youtrack.db.internal.core.metadata.function;

import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunction;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionFactory;
import java.util.Set;

/**
 * Dynamic function factory bound to the database's functions
 */
public class ODatabaseFunctionFactory implements OSQLFunctionFactory {

  @Override
  public void registerDefaultFunctions(YTDatabaseSessionInternal db) {
    // DO NOTHING
  }

  @Override
  public boolean hasFunction(final String iName) {
    var db = ODatabaseRecordThreadLocal.instance().get();
    return db.getMetadata().getFunctionLibrary().getFunction(iName) != null;
  }

  @Override
  public Set<String> getFunctionNames() {
    var db = ODatabaseRecordThreadLocal.instance().get();
    return db.getMetadata().getFunctionLibrary().getFunctionNames();
  }

  @Override
  public OSQLFunction createFunction(final String name) throws YTCommandExecutionException {
    var db = ODatabaseRecordThreadLocal.instance().get();
    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(name);
    return new ODatabaseFunction(f);
  }
}
