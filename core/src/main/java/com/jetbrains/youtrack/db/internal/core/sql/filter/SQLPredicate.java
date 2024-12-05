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
package com.jetbrains.youtrack.db.internal.core.sql.filter;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.parser.BaseParser;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.command.OCommandPredicate;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTQueryParsingException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.OStringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLSelect;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLHelper;
import com.jetbrains.youtrack.db.internal.core.sql.YTCommandSQLParsingException;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OQueryOperator;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OQueryOperatorAnd;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OQueryOperatorNot;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OQueryOperatorOr;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Parses text in SQL format and build a tree of conditions.
 */
public class SQLPredicate extends BaseParser implements OCommandPredicate {

  protected Set<YTProperty> properties = new HashSet<YTProperty>();
  protected OSQLFilterCondition rootCondition;
  protected List<String> recordTransformed;
  protected List<OSQLFilterItemParameter> parameterItems;
  protected int braces;

  @Nonnull
  protected CommandContext context;

  public SQLPredicate(@Nonnull CommandContext context) {
    this.context = context;
  }

  public SQLPredicate(@Nonnull CommandContext context, final String iText) {
    this.context = context;

    text(context.getDatabase(), iText);
  }

  protected void throwSyntaxErrorException(final String iText) {
    final String syntax = getSyntax();
    if (syntax.equals("?")) {
      throw new YTCommandSQLParsingException(iText, parserText, parserGetPreviousPosition());
    }

    throw new YTCommandSQLParsingException(
        iText + ". Use " + syntax, parserText, parserGetPreviousPosition());
  }

  public static String upperCase(String text) {
    StringBuilder result = new StringBuilder(text.length());
    for (char c : text.toCharArray()) {
      String upper = ("" + c).toUpperCase(Locale.ENGLISH);
      if (upper.length() > 1) {
        result.append(c);
      } else {
        result.append(upper);
      }
    }
    return result.toString();
  }

  public SQLPredicate text(YTDatabaseSessionInternal session, final String iText) {
    if (iText == null) {
      throw new YTCommandSQLParsingException("Query text is null");
    }

    try {
      parserText = iText;
      parserTextUpperCase = upperCase(parserText);
      parserSetCurrentPosition(0);
      parserSkipWhiteSpaces();

      rootCondition = (OSQLFilterCondition) extractConditions(session, null);

      optimize(session);
    } catch (YTQueryParsingException e) {
      if (e.getText() == null)
      // QUERY EXCEPTION BUT WITHOUT TEXT: NEST IT
      {
        throw YTException.wrapException(
            new YTQueryParsingException(
                "Error on parsing query", parserText, parserGetCurrentPosition()),
            e);
      }

      throw e;
    } catch (Exception t) {
      throw YTException.wrapException(
          new YTQueryParsingException(
              "Error on parsing query", parserText, parserGetCurrentPosition()),
          t);
    }
    return this;
  }

  public Object evaluate() {
    return evaluate(null, null, null);
  }

  public Object evaluate(final CommandContext iContext) {
    return evaluate(null, null, iContext);
  }

  public Object evaluate(
      final YTIdentifiable iRecord, EntityImpl iCurrentResult, final CommandContext iContext) {
    if (rootCondition == null) {
      return true;
    }

    return rootCondition.evaluate(iRecord, iCurrentResult, iContext);
  }

  protected Object extractConditions(YTDatabaseSessionInternal session,
      final OSQLFilterCondition iParentCondition) {
    final int oldPosition = parserGetCurrentPosition();
    parserNextWord(true, " )=><,\r\n");
    final String word = parserGetLastWord();

    boolean inBraces =
        word.length() > 0 && word.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN;

    if (word.length() > 0
        && (word.equalsIgnoreCase("SELECT") || word.equalsIgnoreCase("TRAVERSE"))) {
      // SUB QUERY
      final StringBuilder embedded = new StringBuilder(256);
      OStringSerializerHelper.getEmbedded(parserText, oldPosition - 1, -1, embedded);
      parserSetCurrentPosition(oldPosition + embedded.length() + 1);
      return new OSQLSynchQuery<Object>(embedded.toString());
    }

    parserSetCurrentPosition(oldPosition);
    OSQLFilterCondition currentCondition = extractCondition(session);

    // CHECK IF THERE IS ANOTHER CONDITION ON RIGHT
    while (parserSkipWhiteSpaces()) {

      if (!parserIsEnded() && parserGetCurrentChar() == ')') {
        return currentCondition;
      }

      final OQueryOperator nextOperator = extractConditionOperator();
      if (nextOperator == null) {
        return currentCondition;
      }

      if (nextOperator.precedence > currentCondition.getOperator().precedence) {
        // SWAP ITEMS
        final OSQLFilterCondition subCondition =
            new OSQLFilterCondition(currentCondition.right, nextOperator);
        currentCondition.right = subCondition;
        subCondition.right = extractConditionItem(session, false, 1);
      } else {
        final OSQLFilterCondition parentCondition =
            new OSQLFilterCondition(currentCondition, nextOperator);
        parentCondition.right = extractConditions(session, parentCondition);
        currentCondition = parentCondition;
      }
    }

    currentCondition.inBraces = inBraces;

    // END OF TEXT
    return currentCondition;
  }

  protected OSQLFilterCondition extractCondition(YTDatabaseSessionInternal session) {

    if (!parserSkipWhiteSpaces())
    // END OF TEXT
    {
      return null;
    }

    // EXTRACT ITEMS
    Object left = extractConditionItem(session, true, 1);

    if (left != null && checkForEnd(left.toString())) {
      return null;
    }

    OQueryOperator oper;
    final Object right;

    if (left instanceof OQueryOperator && ((OQueryOperator) left).isUnary()) {
      oper = (OQueryOperator) left;
      left = extractConditionItem(session, false, 1);
      right = null;
    } else {
      oper = extractConditionOperator();

      if (oper instanceof OQueryOperatorNot)
      // SPECIAL CASE: READ NEXT OPERATOR
      {
        oper = new OQueryOperatorNot(extractConditionOperator());
      }

      if (oper instanceof OQueryOperatorAnd || oper instanceof OQueryOperatorOr) {
        right = extractCondition(session);
      } else {
        right = oper != null ? extractConditionItem(session, false, oper.expectedRightWords) : null;
      }
    }

    // CREATE THE CONDITION OBJECT
    return new OSQLFilterCondition(left, oper, right);
  }

  protected boolean checkForEnd(final String iWord) {
    if (iWord != null
        && (iWord.equals(CommandExecutorSQLSelect.KEYWORD_ORDER)
        || iWord.equals(CommandExecutorSQLSelect.KEYWORD_LIMIT)
        || iWord.equals(CommandExecutorSQLSelect.KEYWORD_SKIP)
        || iWord.equals(CommandExecutorSQLSelect.KEYWORD_OFFSET))) {
      parserMoveCurrentPosition(iWord.length() * -1);
      return true;
    }
    return false;
  }

  private OQueryOperator extractConditionOperator() {
    if (!parserSkipWhiteSpaces())
    // END OF PARSING: JUST RETURN
    {
      return null;
    }

    if (parserGetCurrentChar() == ')')
    // FOUND ')': JUST RETURN
    {
      return null;
    }

    final OQueryOperator[] operators = OSQLEngine.getInstance().getRecordOperators();
    final String[] candidateOperators = new String[operators.length];
    for (int i = 0; i < candidateOperators.length; ++i) {
      candidateOperators[i] = operators[i].keyword;
    }

    final int operatorPos = parserNextChars(true, false, candidateOperators);

    if (operatorPos == -1) {
      parserGoBack();
      return null;
    }

    final OQueryOperator op = operators[operatorPos];
    if (op.expectsParameters) {
      // PARSE PARAMETERS IF ANY
      parserGoBack();

      parserNextWord(true, " 0123456789'\"");
      final String word = parserGetLastWord();

      final List<String> params = new ArrayList<String>();
      // CHECK FOR PARAMETERS
      if (word.length() > op.keyword.length()
          && word.charAt(op.keyword.length()) == OStringSerializerHelper.EMBEDDED_BEGIN) {
        int paramBeginPos = parserGetCurrentPosition() - (word.length() - op.keyword.length());
        parserSetCurrentPosition(
            OStringSerializerHelper.getParameters(parserText, paramBeginPos, -1, params));
      } else if (!word.equals(op.keyword)) {
        throw new YTQueryParsingException(
            "Malformed usage of operator '" + op + "'. Parsed operator is: " + word);
      }

      try {
        // CONFIGURE COULD INSTANTIATE A NEW OBJECT: ACT AS A FACTORY
        return op.configure(params);
      } catch (Exception e) {
        throw YTException.wrapException(
            new YTQueryParsingException(
                "Syntax error using the operator '" + op + "'. Syntax is: " + op.getSyntax()),
            e);
      }
    } else {
      parserMoveCurrentPosition(+1);
    }
    return op;
  }

  private Object extractConditionItem(YTDatabaseSessionInternal session,
      final boolean iAllowOperator,
      final int iExpectedWords) {
    final Object[] result = new Object[iExpectedWords];

    for (int i = 0; i < iExpectedWords; ++i) {
      parserNextWord(false, " =><,\r\n");
      String word = parserGetLastWord();

      if (word.length() == 0) {
        break;
      }

      word = word.replaceAll("\\\\", "\\\\\\\\"); // see issue #5229

      final String uWord = word.toUpperCase(Locale.ENGLISH);

      final int lastPosition = parserIsEnded() ? parserText.length() : parserGetCurrentPosition();

      if (word.length() > 0 && word.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN) {
        braces++;

        // SUB-CONDITION
        parserSetCurrentPosition(lastPosition - word.length() + 1);

        final Object subCondition = extractConditions(session, null);

        if (!parserSkipWhiteSpaces() || parserGetCurrentChar() == ')') {
          braces--;
          parserMoveCurrentPosition(+1);
        }
        if (subCondition instanceof OSQLFilterCondition) {
          ((OSQLFilterCondition) subCondition).inBraces = true;
        }
        result[i] = subCondition;
      } else if (word.charAt(0) == OStringSerializerHelper.LIST_BEGIN) {
        // COLLECTION OF ELEMENTS
        parserSetCurrentPosition(lastPosition - getLastWordLength());

        final List<String> stringItems = new ArrayList<String>();
        parserSetCurrentPosition(
            OStringSerializerHelper.getCollection(
                parserText, parserGetCurrentPosition(), stringItems));
        result[i] = convertCollectionItems(stringItems);

        parserMoveCurrentPosition(+1);

      } else if (uWord.startsWith(
          OSQLFilterItemFieldAll.NAME + OStringSerializerHelper.EMBEDDED_BEGIN)) {

        result[i] = new OSQLFilterItemFieldAll(session, this, word, null);

      } else if (uWord.startsWith(
          OSQLFilterItemFieldAny.NAME + OStringSerializerHelper.EMBEDDED_BEGIN)) {

        result[i] = new OSQLFilterItemFieldAny(session, this, word, null);

      } else {

        if (uWord.equals("NOT")) {
          if (iAllowOperator) {
            return new OQueryOperatorNot();
          } else {
            // GET THE NEXT VALUE
            parserNextWord(false, " )=><,\r\n");
            final String nextWord = parserGetLastWord();

            if (nextWord.length() > 0) {
              word += " " + nextWord;

              if (word.endsWith(")")) {
                word = word.substring(0, word.length() - 1);
              }
            }
          }
        } else if (uWord.equals("AND"))
        // SPECIAL CASE IN "BETWEEN X AND Y"
        {
          result[i] = word;
        }

        while (word.endsWith(")")) {
          final int openParenthesis = word.indexOf('(');
          if (openParenthesis == -1) {
            // DISCARD END PARENTHESIS
            word = word.substring(0, word.length() - 1);
            parserMoveCurrentPosition(-1);
          } else {
            break;
          }
        }

        word = word.replaceAll("\\\\\\\\", "\\\\"); // see issue #5229
        result[i] = OSQLHelper.parseValue(this, this, word, context);
      }
    }

    return iExpectedWords == 1 ? result[0] : result;
  }

  private List<Object> convertCollectionItems(List<String> stringItems) {
    List<Object> coll = new ArrayList<Object>();
    for (String s : stringItems) {
      coll.add(OSQLHelper.parseValue(this, this, s, context));
    }
    return coll;
  }

  public OSQLFilterCondition getRootCondition() {
    return rootCondition;
  }

  @Override
  public String toString() {
    if (rootCondition != null) {
      return "Parsed: " + rootCondition;
    }
    return "Unparsed: " + parserText;
  }

  /**
   * Binds parameters.
   */
  public void bindParameters(final Map<Object, Object> iArgs) {
    if (parameterItems == null || iArgs == null || iArgs.size() == 0) {
      return;
    }

    for (int i = 0; i < parameterItems.size(); i++) {
      OSQLFilterItemParameter value = parameterItems.get(i);
      if ("?".equals(value.getName())) {
        value.setValue(iArgs.get(i));
      } else {
        value.setValue(iArgs.get(value.getName()));
      }
    }
  }

  public OSQLFilterItemParameter addParameter(final String iName) {
    final String name;
    if (iName.charAt(0) == OStringSerializerHelper.PARAMETER_NAMED) {
      name = iName.substring(1);

      // CHECK THE PARAMETER NAME IS CORRECT
      if (!OStringSerializerHelper.isAlphanumeric(name)) {
        throw new YTQueryParsingException(
            "Parameter name '" + name + "' is invalid, only alphanumeric characters are allowed");
      }
    } else {
      name = iName;
    }

    final OSQLFilterItemParameter param = new OSQLFilterItemParameter(name);

    if (parameterItems == null) {
      parameterItems = new ArrayList<OSQLFilterItemParameter>();
    }

    parameterItems.add(param);
    return param;
  }

  public void setRootCondition(final OSQLFilterCondition iCondition) {
    rootCondition = iCondition;
  }

  protected void optimize(YTDatabaseSession session) {
    if (rootCondition != null) {
      computePrefetchFieldList(session, rootCondition, new HashSet<String>());
    }
  }

  protected Set<String> computePrefetchFieldList(
      YTDatabaseSession session, final OSQLFilterCondition iCondition, final Set<String> iFields) {
    Object left = iCondition.getLeft();
    Object right = iCondition.getRight();
    if (left instanceof OSQLFilterItemField) {
      ((OSQLFilterItemField) left).setPreLoadedFields(iFields);
      iFields.add(((OSQLFilterItemField) left).getRoot(session));
    } else if (left instanceof OSQLFilterCondition) {
      computePrefetchFieldList(session, (OSQLFilterCondition) left, iFields);
    }

    if (right instanceof OSQLFilterItemField) {
      ((OSQLFilterItemField) right).setPreLoadedFields(iFields);
      iFields.add(((OSQLFilterItemField) right).getRoot(session));
    } else if (right instanceof OSQLFilterCondition) {
      computePrefetchFieldList(session, (OSQLFilterCondition) right, iFields);
    }

    return iFields;
  }
}
