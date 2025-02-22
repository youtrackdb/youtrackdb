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
package com.jetbrains.youtrack.db.api.exception;

import com.jetbrains.youtrack.db.internal.core.exception.CoreException;

public class CommandScriptException extends CoreException {

  private String text;
  private int position;
  private static final long serialVersionUID = -7430575036316163711L;

  private static String makeMessage(String message, int position, String text) {
    if (text == null) {
      return message;
    }

    final StringBuilder buffer = new StringBuilder();
    buffer.append("Error on parsing script at position #");
    buffer.append(position);
    buffer.append(": " + message);
    buffer.append("\nScript: ");
    buffer.append(text);
    buffer.append("\n------");
    for (int i = 0; i < position - 1; ++i) {
      buffer.append("-");
    }

    buffer.append("^");
    return buffer.toString();
  }

  public CommandScriptException(CommandScriptException exception) {
    super(exception);

    this.text = exception.text;
    this.position = exception.position;
  }

  public CommandScriptException(String iMessage) {
    super(iMessage);
  }

  public CommandScriptException(String iMessage, String iText, int iPosition) {
    super(makeMessage(iMessage, iPosition < 0 ? 0 : iPosition, iText));
    text = iText;
    position = iPosition < 0 ? 0 : iPosition;
  }
}
