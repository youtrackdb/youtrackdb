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
package com.orientechnologies.core.sql.operator;

import com.orientechnologies.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.core.sql.operator.OQueryOperator;
import com.orientechnologies.core.sql.operator.math.OQueryOperatorDivide;
import java.math.BigDecimal;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OQueryOperatorDivideTest {

  @Test
  public void test() {
    OQueryOperator operator = new OQueryOperatorDivide();
    Assert.assertEquals(
        operator.evaluateRecord(
            null, null, null, 10, 3, null, ORecordSerializerBinary.INSTANCE.getCurrentSerializer()),
        10 / 3);
    Assert.assertEquals(
        operator.evaluateRecord(
            null,
            null,
            null,
            10L,
            3L,
            null,
            ORecordSerializerBinary.INSTANCE.getCurrentSerializer()),
        10L / 3L);
    Assert.assertEquals(
        operator.evaluateRecord(
            null,
            null,
            null,
            10.1,
            3,
            null,
            ORecordSerializerBinary.INSTANCE.getCurrentSerializer()),
        10.1 / 3);
    Assert.assertEquals(
        operator.evaluateRecord(
            null,
            null,
            null,
            10,
            3.1,
            null,
            ORecordSerializerBinary.INSTANCE.getCurrentSerializer()),
        10 / 3.1);
    Assert.assertEquals(
        operator.evaluateRecord(
            null,
            null,
            null,
            10.1d,
            3,
            null,
            ORecordSerializerBinary.INSTANCE.getCurrentSerializer()),
        10.1d / 3);
    Assert.assertEquals(
        operator.evaluateRecord(
            null,
            null,
            null,
            10,
            3.1d,
            null,
            ORecordSerializerBinary.INSTANCE.getCurrentSerializer()),
        10 / 3.1d);
    Assert.assertEquals(
        operator.evaluateRecord(
            null,
            null,
            null,
            new BigDecimal(10),
            4,
            null,
            ORecordSerializerBinary.INSTANCE.getCurrentSerializer()),
        new BigDecimal(10).divide(new BigDecimal(4)));
    Assert.assertEquals(
        operator.evaluateRecord(
            null,
            null,
            null,
            10,
            new BigDecimal(4),
            null,
            ORecordSerializerBinary.INSTANCE.getCurrentSerializer()),
        new BigDecimal(10).divide(new BigDecimal(4)));
  }
}
