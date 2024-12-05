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

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.OCollateFactory;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDBInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLTarget;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.method.OSQLMethodFactory;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorFactory;
import com.orientechnologies.orient.core.sql.parser.OOrBlock;
import com.orientechnologies.orient.core.sql.parser.OSecurityResourceSegment;
import com.orientechnologies.orient.core.sql.parser.OServerStatement;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.sql.parser.OrientSql;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

public class OSQLEngine {

  protected static final OSQLEngine INSTANCE = new OSQLEngine();
  private static volatile List<OSQLFunctionFactory> FUNCTION_FACTORIES = null;
  private static List<OSQLMethodFactory> METHOD_FACTORIES = null;
  private static List<OCommandExecutorSQLFactory> EXECUTOR_FACTORIES = null;
  private static List<OQueryOperatorFactory> OPERATOR_FACTORIES = null;
  private static List<OCollateFactory> COLLATE_FACTORIES = null;
  private static OQueryOperator[] SORTED_OPERATORS = null;
  private static final ClassLoader orientClassLoader = OSQLEngine.class.getClassLoader();

  public static OStatement parse(String query, YTDatabaseSessionInternal db) {
    return OStatementCache.get(query, db);
  }

  public static OServerStatement parseServerStatement(String query, YouTrackDBInternal db) {
    return OStatementCache.getServerStatement(query, db);
  }

  public static List<OStatement> parseScript(String script, YTDatabaseSessionInternal db) {
    final InputStream is = new ByteArrayInputStream(script.getBytes());
    return parseScript(is, db);
  }

  public static List<OStatement> parseScript(InputStream script, YTDatabaseSessionInternal db) {
    try {
      final OrientSql osql = new OrientSql(script);
      List<OStatement> result = osql.parseScript();
      return result;
    } catch (ParseException e) {
      throw new YTCommandSQLParsingException(e, "");
    }
  }

  public static OOrBlock parsePredicate(String predicate) throws YTCommandSQLParsingException {
    final InputStream is = new ByteArrayInputStream(predicate.getBytes());
    try {
      final OrientSql osql = new OrientSql(is);
      OOrBlock result = osql.OrBlock();
      return result;
    } catch (ParseException e) {
      throw new YTCommandSQLParsingException(e, "");
    }
  }

  public static OSecurityResourceSegment parseSecurityResource(String exp) {
    final InputStream is = new ByteArrayInputStream(exp.getBytes());
    try {
      final OrientSql osql = new OrientSql(is);
      OSecurityResourceSegment result = osql.SecurityResourceSegment();
      return result;
    } catch (ParseException e) {
      throw new YTCommandSQLParsingException(e, "");
    }
  }

  /**
   * internal use only, to sort operators.
   */
  private static final class Pair {

    private final OQueryOperator before;
    private final OQueryOperator after;

    public Pair(final OQueryOperator before, final OQueryOperator after) {
      this.before = before;
      this.after = after;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj instanceof Pair that) {
        return before == that.before && after == that.after;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(before) + 31 * System.identityHashCode(after);
    }

    @Override
    public String toString() {
      return before + " > " + after;
    }
  }

  protected OSQLEngine() {
  }

  public static void registerOperator(final OQueryOperator iOperator) {
    ODynamicSQLElementFactory.OPERATORS.add(iOperator);
    SORTED_OPERATORS = null; // clear cache
  }

  /**
   * @return Iterator of all function factories
   */
  public static Iterator<OSQLFunctionFactory> getFunctionFactories(
      YTDatabaseSessionInternal session) {
    if (FUNCTION_FACTORIES == null) {
      synchronized (INSTANCE) {
        if (FUNCTION_FACTORIES == null) {
          final Iterator<OSQLFunctionFactory> ite =
              lookupProviderWithOrientClassLoader(OSQLFunctionFactory.class, orientClassLoader);

          final List<OSQLFunctionFactory> factories = new ArrayList<OSQLFunctionFactory>();
          while (ite.hasNext()) {
            var factory = ite.next();
            try {
              factory.registerDefaultFunctions(session);
              factories.add(factory);
            } catch (Exception e) {
              OLogManager.instance().warn(OSQLEngine.class,
                  "Cannot register default functions for function factory " + factory, e);
            }
          }
          FUNCTION_FACTORIES = Collections.unmodifiableList(factories);
        }
      }
    }
    return FUNCTION_FACTORIES.iterator();
  }

  public static Iterator<OSQLMethodFactory> getMethodFactories() {
    if (METHOD_FACTORIES == null) {
      synchronized (INSTANCE) {
        if (METHOD_FACTORIES == null) {

          final Iterator<OSQLMethodFactory> ite =
              lookupProviderWithOrientClassLoader(OSQLMethodFactory.class, orientClassLoader);

          final List<OSQLMethodFactory> factories = new ArrayList<OSQLMethodFactory>();
          while (ite.hasNext()) {
            factories.add(ite.next());
          }
          METHOD_FACTORIES = Collections.unmodifiableList(factories);
        }
      }
    }
    return METHOD_FACTORIES.iterator();
  }

  /**
   * @return Iterator of all function factories
   */
  public static Iterator<OCollateFactory> getCollateFactories() {
    if (COLLATE_FACTORIES == null) {
      synchronized (INSTANCE) {
        if (COLLATE_FACTORIES == null) {

          final Iterator<OCollateFactory> ite =
              lookupProviderWithOrientClassLoader(OCollateFactory.class, orientClassLoader);

          final List<OCollateFactory> factories = new ArrayList<OCollateFactory>();
          while (ite.hasNext()) {
            factories.add(ite.next());
          }
          COLLATE_FACTORIES = Collections.unmodifiableList(factories);
        }
      }
    }
    return COLLATE_FACTORIES.iterator();
  }

  /**
   * @return Iterator of all operator factories
   */
  public static Iterator<OQueryOperatorFactory> getOperatorFactories() {
    if (OPERATOR_FACTORIES == null) {
      synchronized (INSTANCE) {
        if (OPERATOR_FACTORIES == null) {

          final Iterator<OQueryOperatorFactory> ite =
              lookupProviderWithOrientClassLoader(OQueryOperatorFactory.class, orientClassLoader);

          final List<OQueryOperatorFactory> factories = new ArrayList<OQueryOperatorFactory>();
          while (ite.hasNext()) {
            factories.add(ite.next());
          }
          OPERATOR_FACTORIES = Collections.unmodifiableList(factories);
        }
      }
    }
    return OPERATOR_FACTORIES.iterator();
  }

  /**
   * @return Iterator of all command factories
   */
  public static Iterator<OCommandExecutorSQLFactory> getCommandFactories() {
    if (EXECUTOR_FACTORIES == null) {
      synchronized (INSTANCE) {
        if (EXECUTOR_FACTORIES == null) {

          final Iterator<OCommandExecutorSQLFactory> ite =
              lookupProviderWithOrientClassLoader(
                  OCommandExecutorSQLFactory.class, orientClassLoader);
          final List<OCommandExecutorSQLFactory> factories =
              new ArrayList<OCommandExecutorSQLFactory>();
          while (ite.hasNext()) {
            try {
              factories.add(ite.next());
            } catch (Exception e) {
              OLogManager.instance()
                  .warn(
                      OSQLEngine.class,
                      "Cannot load OCommandExecutorSQLFactory instance from service registry",
                      e);
            }
          }

          EXECUTOR_FACTORIES = Collections.unmodifiableList(factories);
        }
      }
    }
    return EXECUTOR_FACTORIES.iterator();
  }

  /**
   * Iterates on all factories and append all function names.
   *
   * @return Set of all function names.
   */
  public static Set<String> getFunctionNames(YTDatabaseSessionInternal session) {
    final Set<String> types = new HashSet<String>();
    final Iterator<OSQLFunctionFactory> ite = getFunctionFactories(session);
    while (ite.hasNext()) {
      types.addAll(ite.next().getFunctionNames());
    }
    return types;
  }

  public static Set<String> getMethodNames() {
    final Set<String> types = new HashSet<String>();
    final Iterator<OSQLMethodFactory> ite = getMethodFactories();
    while (ite.hasNext()) {
      types.addAll(ite.next().getMethodNames());
    }
    return types;
  }

  /**
   * Iterates on all factories and append all collate names.
   *
   * @return Set of all colate names.
   */
  public static Set<String> getCollateNames() {
    final Set<String> types = new HashSet<String>();
    final Iterator<OCollateFactory> ite = getCollateFactories();
    while (ite.hasNext()) {
      types.addAll(ite.next().getNames());
    }
    return types;
  }

  /**
   * Iterates on all factories and append all command names.
   *
   * @return Set of all command names.
   */
  public static Set<String> getCommandNames() {
    final Set<String> types = new HashSet<String>();
    final Iterator<OCommandExecutorSQLFactory> ite = getCommandFactories();
    while (ite.hasNext()) {
      types.addAll(ite.next().getCommandNames());
    }
    return types;
  }

  /**
   * Scans for factory plug-ins on the application class path. This method is needed because the
   * application class path can theoretically change, or additional plug-ins may become available.
   * Rather than re-scanning the classpath on every invocation of the API, the class path is scanned
   * automatically only on the first invocation. Clients can call this method to prompt a re-scan.
   * Thus this method need only be invoked by sophisticated applications which dynamically make new
   * plug-ins available at runtime.
   */
  public static void scanForPlugins() {
    // clear cache, will cause a rescan on next getFunctionFactories call
    FUNCTION_FACTORIES = null;
  }

  public static Object foreachRecord(
      final OCallable<Object, YTIdentifiable> iCallable,
      Object iCurrent,
      final OCommandContext iContext) {
    if (iCurrent == null) {
      return null;
    }

    if (!OCommandExecutorAbstract.checkInterruption(iContext)) {
      return null;
    }

    if (iCurrent instanceof Iterable && !(iCurrent instanceof YTIdentifiable)) {
      iCurrent = ((Iterable) iCurrent).iterator();
    }
    if (OMultiValue.isMultiValue(iCurrent) || iCurrent instanceof Iterator) {
      final OMultiCollectionIterator<Object> result = new OMultiCollectionIterator<Object>();
      for (Object o : OMultiValue.getMultiValueIterable(iCurrent)) {
        if (iContext != null && !iContext.checkTimeout()) {
          return null;
        }

        if (OMultiValue.isMultiValue(o) || o instanceof Iterator) {
          for (Object inner : OMultiValue.getMultiValueIterable(o)) {
            result.add(iCallable.call((YTIdentifiable) inner));
          }
        } else {
          result.add(iCallable.call((YTIdentifiable) o));
        }
      }
      return result;
    } else if (iCurrent instanceof YTIdentifiable) {
      return iCallable.call((YTIdentifiable) iCurrent);
    } else if (iCurrent instanceof YTResult) {
      return iCallable.call(((YTResult) iCurrent).toEntity());
    }

    return null;
  }

  public static OSQLEngine getInstance() {
    return INSTANCE;
  }

  public static OCollate getCollate(final String name) {
    for (Iterator<OCollateFactory> iter = getCollateFactories(); iter.hasNext(); ) {
      OCollateFactory f = iter.next();
      final OCollate c = f.getCollate(name);
      if (c != null) {
        return c;
      }
    }
    return null;
  }

  public static OSQLMethod getMethod(String iMethodName) {
    iMethodName = iMethodName.toLowerCase(Locale.ENGLISH);

    final Iterator<OSQLMethodFactory> ite = getMethodFactories();
    while (ite.hasNext()) {
      final OSQLMethodFactory factory = ite.next();
      if (factory.hasMethod(iMethodName)) {
        return factory.createMethod(iMethodName);
      }
    }

    return null;
  }

  public OQueryOperator[] getRecordOperators() {
    if (SORTED_OPERATORS == null) {
      synchronized (INSTANCE) {
        if (SORTED_OPERATORS == null) {
          // sort operators, will happen only very few times since we cache the
          // result
          final Iterator<OQueryOperatorFactory> ite = getOperatorFactories();
          final List<OQueryOperator> operators = new ArrayList<OQueryOperator>();
          while (ite.hasNext()) {
            final OQueryOperatorFactory factory = ite.next();
            operators.addAll(factory.getOperators());
          }

          final List<OQueryOperator> sorted = new ArrayList<OQueryOperator>();
          final Set<Pair> pairs = new LinkedHashSet<Pair>();
          for (final OQueryOperator ca : operators) {
            for (final OQueryOperator cb : operators) {
              if (ca != cb) {
                switch (ca.compare(cb)) {
                  case BEFORE:
                    pairs.add(new Pair(ca, cb));
                    break;
                  case AFTER:
                    pairs.add(new Pair(cb, ca));
                    break;
                }
                switch (cb.compare(ca)) {
                  case BEFORE:
                    pairs.add(new Pair(cb, ca));
                    break;
                  case AFTER:
                    pairs.add(new Pair(ca, cb));
                    break;
                }
              }
            }
          }
          boolean added;
          do {
            added = false;
            scan:
            for (final Iterator<OQueryOperator> it = operators.iterator(); it.hasNext(); ) {
              final OQueryOperator candidate = it.next();
              for (final Pair pair : pairs) {
                if (pair.after == candidate) {
                  continue scan;
                }
              }
              sorted.add(candidate);
              it.remove();
              for (final Iterator<Pair> itp = pairs.iterator(); itp.hasNext(); ) {
                if (itp.next().before == candidate) {
                  itp.remove();
                }
              }
              added = true;
            }
          } while (added);
          if (!operators.isEmpty()) {
            throw new YTDatabaseException("Invalid sorting. " + OCollections.toString(pairs));
          }
          SORTED_OPERATORS = sorted.toArray(new OQueryOperator[sorted.size()]);
        }
      }
    }
    return SORTED_OPERATORS;
  }

  public void registerFunction(final String iName, final OSQLFunction iFunction) {
    ODynamicSQLElementFactory.FUNCTIONS.put(iName.toLowerCase(Locale.ENGLISH), iFunction);
  }

  public void registerFunction(
      final String iName, final Class<? extends OSQLFunction> iFunctionClass) {
    ODynamicSQLElementFactory.FUNCTIONS.put(iName.toLowerCase(Locale.ENGLISH), iFunctionClass);
  }

  public OSQLFunction getFunction(YTDatabaseSessionInternal session, String iFunctionName) {
    iFunctionName = iFunctionName.toLowerCase(Locale.ENGLISH);

    if (iFunctionName.equalsIgnoreCase("any") || iFunctionName.equalsIgnoreCase("all"))
    // SPECIAL FUNCTIONS
    {
      return null;
    }

    final Iterator<OSQLFunctionFactory> ite = getFunctionFactories(session);
    while (ite.hasNext()) {
      final OSQLFunctionFactory factory = ite.next();
      if (factory.hasFunction(iFunctionName)) {
        return factory.createFunction(iFunctionName);
      }
    }

    throw new YTCommandSQLParsingException(
        "No function with name '"
            + iFunctionName
            + "', available names are : "
            + OCollections.toString(getFunctionNames(session)));
  }

  public void unregisterFunction(String iName) {
    iName = iName.toLowerCase(Locale.ENGLISH);
    ODynamicSQLElementFactory.FUNCTIONS.remove(iName);
  }

  public OCommandExecutor getCommand(String candidate) {
    candidate = candidate.trim();
    final Set<String> names = getCommandNames();
    String commandName = candidate;
    boolean found = names.contains(commandName);
    int pos = -1;
    while (!found) {
      pos =
          OStringSerializerHelper.getLowerIndexOf(
              candidate, pos + 1, " ", "\n", "\r", "\t", "(", "[");
      if (pos > -1) {
        commandName = candidate.substring(0, pos);
        // remove double spaces
        commandName = commandName.replaceAll(" +", " ");
        found = names.contains(commandName);
      } else {
        break;
      }
    }

    if (found) {
      final Iterator<OCommandExecutorSQLFactory> ite = getCommandFactories();
      while (ite.hasNext()) {
        final OCommandExecutorSQLFactory factory = ite.next();
        if (factory.getCommandNames().contains(commandName)) {
          return factory.createCommand(commandName);
        }
      }
    }

    return null;
  }

  public static OSQLFilter parseCondition(
      final String iText, @Nonnull final OCommandContext iContext, final String iFilterKeyword) {
    assert iContext != null;
    return new OSQLFilter(iText, iContext, iFilterKeyword);
  }

  public static OSQLTarget parseTarget(final String iText, final OCommandContext iContext) {
    return new OSQLTarget(iText, iContext);
  }

  public Set<YTIdentifiable> parseRIDTarget(
      final YTDatabaseSession database,
      String iTarget,
      final OCommandContext iContext,
      Map<Object, Object> iArgs) {
    final Set<YTIdentifiable> ids;
    if (iTarget.startsWith("(")) {
      // SUB-QUERY
      final OSQLSynchQuery<Object> query =
          new OSQLSynchQuery<Object>(iTarget.substring(1, iTarget.length() - 1));
      query.setContext(iContext);

      final List<YTIdentifiable> result = ((YTDatabaseSessionInternal) database).query(query,
          iArgs);
      if (result == null || result.isEmpty()) {
        ids = Collections.emptySet();
      } else {
        ids = new HashSet<YTIdentifiable>((int) (result.size() * 1.3));
        for (YTIdentifiable aResult : result) {
          ids.add(aResult.getIdentity());
        }
      }
    } else if (iTarget.startsWith("[")) {
      // COLLECTION OF RIDS
      final String[] idsAsStrings = iTarget.substring(1, iTarget.length() - 1).split(",");
      ids = new HashSet<YTIdentifiable>((int) (idsAsStrings.length * 1.3));
      for (String idsAsString : idsAsStrings) {
        if (idsAsString.startsWith("$")) {
          Object r = iContext.getVariable(idsAsString);
          if (r instanceof YTIdentifiable) {
            ids.add((YTIdentifiable) r);
          } else {
            OMultiValue.add(ids, r);
          }
        } else {
          ids.add(new YTRecordId(idsAsString));
        }
      }
    } else {
      // SINGLE RID
      if (iTarget.startsWith("$")) {
        Object r = iContext.getVariable(iTarget);
        if (r instanceof YTIdentifiable) {
          ids = Collections.singleton((YTIdentifiable) r);
        } else {
          ids =
              (Set<YTIdentifiable>)
                  OMultiValue.add(new HashSet<YTIdentifiable>(OMultiValue.getSize(r)), r);
        }

      } else {
        ids = Collections.singleton(new YTRecordId(iTarget));
      }
    }
    return ids;
  }
}
