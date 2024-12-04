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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLMethodMultiValue;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.method.OSQLMethodRuntime;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodField;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodFunctionDelegate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nonnull;

/**
 * Represents an object field as value in the query condition.
 */
public abstract class OSQLFilterItemAbstract implements OSQLFilterItem {

  protected List<OPair<OSQLMethodRuntime, Object[]>> operationsChain = null;

  protected OSQLFilterItemAbstract() {
  }

  public OSQLFilterItemAbstract(YTDatabaseSessionInternal session, final OBaseParser iQueryToParse,
      final String iText) {
    final List<String> parts =
        OStringSerializerHelper.smartSplit(
            iText,
            new char[]{'.', '[', ']'},
            new boolean[]{false, false, true},
            new boolean[]{false, true, false},
            0,
            -1,
            false,
            true,
            false,
            false,
            OCommonConst.EMPTY_CHAR_ARRAY);

    setRoot(session, iQueryToParse, parts.get(0));

    if (parts.size() > 1) {
      operationsChain = new ArrayList<OPair<OSQLMethodRuntime, Object[]>>();

      // GET ALL SPECIAL OPERATIONS
      for (int i = 1; i < parts.size(); ++i) {
        final String part = parts.get(i);

        final int pindex = part.indexOf('(');
        if (part.charAt(0) == '[') {
          operationsChain.add(
              new OPair<OSQLMethodRuntime, Object[]>(
                  new OSQLMethodRuntime(OSQLEngine.getMethod(OSQLMethodMultiValue.NAME)),
                  new Object[]{part}));
        } else if (pindex > -1) {
          final String methodName = part.substring(0, pindex).trim().toLowerCase(Locale.ENGLISH);

          OSQLMethod method = OSQLEngine.getMethod(methodName);
          final Object[] arguments;
          if (method != null) {
            if (method.getMaxParams(session) == -1 || method.getMaxParams(session) > 0) {
              arguments = OStringSerializerHelper.getParameters(part).toArray();
              if (arguments.length < method.getMinParams()
                  || (method.getMaxParams(session) > -1 && arguments.length > method.getMaxParams(
                  session))) {
                String params;
                if (method.getMinParams() == method.getMaxParams(session)) {
                  params = "" + method.getMinParams();
                } else {
                  params = method.getMinParams() + "-" + method.getMaxParams(session);
                }
                throw new OQueryParsingException(
                    iQueryToParse.parserText,
                    "Syntax error: field operator '"
                        + method.getName()
                        + "' needs "
                        + params
                        + " argument(s) while has been received "
                        + arguments.length,
                    0);
              }
            } else {
              arguments = null;
            }

          } else {
            // LOOK FOR FUNCTION
            final OSQLFunction f = OSQLEngine.getInstance().getFunction(session, methodName);

            if (f == null)
            // ERROR: METHOD/FUNCTION NOT FOUND OR MISPELLED
            {
              throw new OQueryParsingException(
                  iQueryToParse.parserText,
                  "Syntax error: function or field operator not recognized between the supported"
                      + " ones: "
                      + OSQLEngine.getMethodNames(),
                  0);
            }

            if (f.getMaxParams(session) == -1 || f.getMaxParams(session) > 0) {
              arguments = OStringSerializerHelper.getParameters(part).toArray();
              if (arguments.length + 1 < f.getMinParams()
                  || (f.getMaxParams(session) > -1 && arguments.length + 1 > f.getMaxParams(
                  session))) {
                String params;
                if (f.getMinParams() == f.getMaxParams(session)) {
                  params = "" + f.getMinParams();
                } else {
                  params = f.getMinParams() + "-" + f.getMaxParams(session);
                }
                throw new OQueryParsingException(
                    iQueryToParse.parserText,
                    "Syntax error: function '"
                        + f.getName(session)
                        + "' needs "
                        + params
                        + " argument(s) while has been received "
                        + arguments.length,
                    0);
              }
            } else {
              arguments = null;
            }

            method = new OSQLMethodFunctionDelegate(f);
          }

          final OSQLMethodRuntime runtimeMethod = new OSQLMethodRuntime(method);

          // SPECIAL OPERATION FOUND: ADD IT IN TO THE CHAIN
          operationsChain.add(new OPair<OSQLMethodRuntime, Object[]>(runtimeMethod, arguments));

        } else {
          operationsChain.add(
              new OPair<OSQLMethodRuntime, Object[]>(
                  new OSQLMethodRuntime(OSQLEngine.getMethod(OSQLMethodField.NAME)),
                  new Object[]{part}));
        }
      }
    }
  }

  public abstract String getRoot(YTDatabaseSession session);

  public Object transformValue(
      final YTIdentifiable iRecord, @Nonnull final OCommandContext iContext, Object ioResult) {
    if (ioResult != null && operationsChain != null) {
      // APPLY OPERATIONS FOLLOWING THE STACK ORDER
      OSQLMethodRuntime method = null;

      for (OPair<OSQLMethodRuntime, Object[]> op : operationsChain) {
        method = op.getKey();

        // DON'T PASS THE CURRENT RECORD TO FORCE EVALUATING TEMPORARY RESULT
        method.setParameters(iContext.getDatabase(), op.getValue(), true);

        ioResult = method.execute(ioResult, iRecord, ioResult, iContext);
      }
    }

    return ioResult;
  }

  public boolean hasChainOperators() {
    return operationsChain != null;
  }

  public OPair<OSQLMethodRuntime, Object[]> getLastChainOperator() {
    if (operationsChain != null) {
      return operationsChain.get(operationsChain.size() - 1);
    }

    return null;
  }

  @Override
  public String toString() {
    var db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      final StringBuilder buffer = new StringBuilder(128);
      final String root = getRoot(db);
      if (root != null) {
        buffer.append(root);
      }
      if (operationsChain != null) {
        for (OPair<OSQLMethodRuntime, Object[]> op : operationsChain) {
          buffer.append('.');
          buffer.append(op.getKey());
          if (op.getValue() != null) {
            final Object[] values = op.getValue();
            buffer.append('(');
            int i = 0;
            for (Object v : values) {
              if (i++ > 0) {
                buffer.append(',');
              }
              buffer.append(v);
            }
            buffer.append(')');
          }
        }
      }
      return buffer.toString();
    }

    return super.toString();
  }

  protected abstract void setRoot(YTDatabaseSessionInternal session, OBaseParser iQueryToParse,
      final String iRoot);

  protected OCollate getCollateForField(final YTClass iClass, final String iFieldName) {
    if (iClass != null) {
      final YTProperty p = iClass.getProperty(iFieldName);
      if (p != null) {
        return p.getCollate();
      }
    }
    return null;
  }
}
