/*
 *
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.fetch.json;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import java.io.IOException;

/**
 *
 */
public class OJSONFetchListener implements OFetchListener {

  public boolean requireFieldProcessing() {
    return true;
  }

  public void processStandardField(
      final YTEntityImpl iRecord,
      final Object iFieldValue,
      final String iFieldName,
      final OFetchContext iContext,
      final Object iusObject,
      final String iFormat,
      YTType filedType) {
    try {
      ((OJSONFetchContext) iContext)
          .getJsonWriter()
          .writeAttribute(
              ((OJSONFetchContext) iContext).getIndentLevel() + 1,
              true,
              iFieldName,
              iFieldValue,
              iFormat,
              filedType);
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTFetchException(
              "Error processing field '" + iFieldValue + " of record " + iRecord.getIdentity()),
          e);
    }
  }

  public void processStandardCollectionValue(final Object iFieldValue, final OFetchContext iContext)
      throws YTFetchException {
    try {
      ((OJSONFetchContext) iContext)
          .getJsonWriter()
          .writeValue(
              ((OJSONFetchContext) iContext).getIndentLevel(),
              true,
              OJSONWriter.encode(iFieldValue));
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on processStandardCollectionValue", e);
    }
  }

  public Object fetchLinked(
      final YTEntityImpl iRecord,
      final Object iUserObject,
      final String iFieldName,
      final YTEntityImpl iLinked,
      final OFetchContext iContext)
      throws YTFetchException {
    return iLinked;
  }

  public Object fetchLinkedMapEntry(
      final YTEntityImpl iRecord,
      final Object iUserObject,
      final String iFieldName,
      final String iKey,
      final YTEntityImpl iLinked,
      final OFetchContext iContext)
      throws YTFetchException {
    return iLinked;
  }

  public void parseLinked(
      final YTEntityImpl iRootRecord,
      final YTIdentifiable iLinked,
      final Object iUserObject,
      final String iFieldName,
      final OFetchContext iContext)
      throws YTFetchException {
    try {
      ((OJSONFetchContext) iContext).writeLinkedAttribute(iLinked, iFieldName);
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTFetchException(
              "Error writing linked field "
                  + iFieldName
                  + " (record:"
                  + iLinked.getIdentity()
                  + ") of record "
                  + iRootRecord.getIdentity()),
          e);
    }
  }

  public void parseLinkedCollectionValue(
      YTEntityImpl iRootRecord,
      YTIdentifiable iLinked,
      Object iUserObject,
      String iFieldName,
      OFetchContext iContext)
      throws YTFetchException {
    try {
      if (((OJSONFetchContext) iContext).isInCollection(iRootRecord)) {
        ((OJSONFetchContext) iContext).writeLinkedValue(iLinked, iFieldName);
      } else {
        ((OJSONFetchContext) iContext).writeLinkedAttribute(iLinked, iFieldName);
      }
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTFetchException(
              "Error writing linked field "
                  + iFieldName
                  + " (record:"
                  + iLinked.getIdentity()
                  + ") of record "
                  + iRootRecord.getIdentity()),
          e);
    }
  }

  public Object fetchLinkedCollectionValue(
      YTEntityImpl iRoot,
      Object iUserObject,
      String iFieldName,
      YTEntityImpl iLinked,
      OFetchContext iContext)
      throws YTFetchException {
    return iLinked;
  }

  @Override
  public void skipStandardField(
      YTEntityImpl iRecord,
      String iFieldName,
      OFetchContext iContext,
      Object iUserObject,
      String iFormat)
      throws YTFetchException {
  }
}
