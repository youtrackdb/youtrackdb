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
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.command.OCommandPredicate;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTQueryParsingException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Locale;
import javax.annotation.Nonnull;

/**
 * Parsed query. It's built once a query is parsed.
 */
public class SQLFilter extends SQLPredicate implements OCommandPredicate {

  public SQLFilter(
      final String iText, @Nonnull final CommandContext iContext, final String iFilterKeyword) {
    super(iContext);

    if (iText == null) {
      throw new IllegalArgumentException("Filter expression is null");
    }

    parserText = iText;
    parserTextUpperCase = iText.toUpperCase(Locale.ENGLISH);

    try {
      final int lastPos = parserGetCurrentPosition();
      final String lastText = parserText;
      final String lastTextUpperCase = parserTextUpperCase;

      text(iContext.getDatabase(), parserText.substring(lastPos));

      parserText = lastText;
      parserTextUpperCase = lastTextUpperCase;
      parserMoveCurrentPosition(lastPos);

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
    } catch (Exception e) {
      throw YTException.wrapException(
          new YTQueryParsingException(
              "Error on parsing query", parserText, parserGetCurrentPosition()),
          e);
    }

    this.rootCondition = resetOperatorPrecedence(rootCondition);
  }

  private OSQLFilterCondition resetOperatorPrecedence(OSQLFilterCondition iCondition) {
    if (iCondition == null) {
      return iCondition;
    }
    if (iCondition.left != null && iCondition.left instanceof OSQLFilterCondition) {
      iCondition.left = resetOperatorPrecedence((OSQLFilterCondition) iCondition.left);
    }

    if (iCondition.right != null && iCondition.right instanceof OSQLFilterCondition right) {
      iCondition.right = resetOperatorPrecedence(right);
      if (iCondition.operator != null) {
        if (!right.inBraces
            && right.operator != null
            && right.operator.precedence < iCondition.operator.precedence) {
          OSQLFilterCondition newLeft =
              new OSQLFilterCondition(iCondition.left, iCondition.operator, right.left);
          right.setLeft(newLeft);
          resetOperatorPrecedence(right);
          return right;
        }
      }
    }

    return iCondition;
  }

  public Object evaluate(
      final YTIdentifiable iRecord, final EntityImpl iCurrentResult,
      final CommandContext iContext) {
    if (rootCondition == null) {
      return true;
    }

    return rootCondition.evaluate(iRecord, iCurrentResult, iContext);
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
}
