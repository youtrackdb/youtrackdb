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
package com.jetbrains.youtrack.db.internal.core.metadata;

import com.jetbrains.youtrack.db.internal.core.metadata.function.OFunctionLibrary;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurity;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.OSequenceLibrary;
import com.jetbrains.youtrack.db.internal.core.schedule.OScheduler;

/**
 *
 */
public interface OMetadata {

  YTSchema getSchema();

  OSecurity getSecurity();

  OFunctionLibrary getFunctionLibrary();

  OSequenceLibrary getSequenceLibrary();

  OScheduler getScheduler();
}
