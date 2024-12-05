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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;

/**
 *
 */
public class OUpdatedRecordsReturnHandler extends ORecordsReturnHandler {

  public OUpdatedRecordsReturnHandler(Object returnExpression, OCommandContext context) {
    super(returnExpression, context);
  }

  @Override
  protected YTEntityImpl preprocess(YTEntityImpl result) {
    return result;
  }

  @Override
  public void beforeUpdate(YTEntityImpl result) {
  }

  @Override
  public void afterUpdate(YTEntityImpl result) {
    storeResult(result);
  }
}
