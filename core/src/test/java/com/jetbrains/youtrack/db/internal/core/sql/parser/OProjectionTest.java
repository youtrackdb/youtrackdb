/*
 *
 *  *  Copyright YouTrackDB
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
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.sql.YTCommandSQLParsingException;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OSelectStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OrientSql;
import com.jetbrains.youtrack.db.internal.core.sql.parser.ParseException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OProjectionTest {

  @Test
  public void testIsExpand() throws ParseException {
    OrientSql parser = getParserFor("select expand(foo)  from V");
    OSelectStatement stm = (OSelectStatement) parser.parse();
    Assert.assertTrue(stm.getProjection().isExpand());

    OrientSql parser2 = getParserFor("select foo  from V");
    OSelectStatement stm2 = (OSelectStatement) parser2.parse();
    Assert.assertFalse(stm2.getProjection().isExpand());

    OrientSql parser3 = getParserFor("select expand  from V");
    OSelectStatement stm3 = (OSelectStatement) parser3.parse();
    Assert.assertFalse(stm3.getProjection().isExpand());
  }

  @Test
  public void testValidate() throws ParseException {
    OrientSql parser = getParserFor("select expand(foo)  from V");
    OSelectStatement stm = (OSelectStatement) parser.parse();
    stm.getProjection().validate();

    try {
      getParserFor("select expand(foo), bar  from V").parse();
      Assert.fail();
    } catch (YTCommandSQLParsingException ex) {

    } catch (Exception x) {
      Assert.fail();
    }
  }

  protected OrientSql getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    OrientSql osql = new OrientSql(is);
    return osql;
  }
}
