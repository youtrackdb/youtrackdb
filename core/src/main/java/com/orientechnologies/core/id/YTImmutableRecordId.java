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
package com.orientechnologies.core.id;

import com.orientechnologies.core.serialization.OMemoryStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Immutable YTRID implementation. To be really immutable fields must not be public anymore. TODO!
 */
public class YTImmutableRecordId extends YTRecordId {

  public static final YTRID EMPTY_RECORD_ID = new YTImmutableRecordId();
  private static final long serialVersionUID = 1L;

  public YTImmutableRecordId() {
    super();
  }

  public YTImmutableRecordId(final int iClusterId, final long iClusterPosition) {
    super(iClusterId, iClusterPosition);
  }

  public YTImmutableRecordId(final YTRecordId iRID) {
    super(iRID);
  }

  @Override
  public void copyFrom(final YTRID iSource) {
    throw new UnsupportedOperationException("copyFrom");
  }

  @Override
  public YTRecordId fromStream(byte[] iBuffer) {
    throw new UnsupportedOperationException("fromStream");
  }

  @Override
  public YTRecordId fromStream(OMemoryStream iStream) {
    throw new UnsupportedOperationException("fromStream");
  }

  @Override
  public YTRecordId fromStream(InputStream iStream) throws IOException {
    throw new UnsupportedOperationException("fromStream");
  }

  @Override
  public void fromString(String iRecordId) {
    throw new UnsupportedOperationException("fromString");
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("reset");
  }
}
