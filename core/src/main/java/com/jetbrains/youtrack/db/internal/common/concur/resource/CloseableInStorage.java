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
package com.jetbrains.youtrack.db.internal.common.concur.resource;

public interface CloseableInStorage {

  /**
   * Closes resources inside of call of Storage#close(). So do not use locks when you call this
   * method, or you may have deadlock during storage close. This method is completely housekeeping
   * method and plays role of Object#finalize() in case of you need to clean up resources after
   * storage is closed.
   */
  void close();
}
