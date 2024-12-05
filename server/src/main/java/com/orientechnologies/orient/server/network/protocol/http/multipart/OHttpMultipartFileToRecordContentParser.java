/*
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
package com.orientechnologies.orient.server.network.protocol.http.multipart;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.record.impl.Blob;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class OHttpMultipartFileToRecordContentParser implements OHttpMultipartContentParser<YTRID> {

  @Override
  public YTRID parse(
      final OHttpRequest iRequest,
      final Map<String, String> headers,
      final OHttpMultipartContentInputStream in,
      YTDatabaseSession database)
      throws IOException {
    final Blob record = new RecordBytes();
    record.fromInputStream(in);
    record.save();
    return record.getIdentity();
  }
}
