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
package com.jetbrains.youtrack.db.internal.server.network.protocol.http.multipart;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.OHttpRequest;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class HttpMultipartFileToRecordContentParser implements
    HttpMultipartContentParser<RecordId> {

  @Override
  public RecordId parse(
      final OHttpRequest iRequest,
      final Map<String, String> headers,
      final HttpMultipartContentInputStream in,
      DatabaseSession database)
      throws IOException {
    var record = new RecordBytes();
    record.fromInputStream(in);
    record.save();
    return record.getIdentity();
  }
}
