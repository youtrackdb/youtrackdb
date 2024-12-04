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
package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.metadata.sequence.YTSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.impl.YTDocument;

/**
 * @since 3/1/2015
 */
public class OSequenceHelper {

  public static final SEQUENCE_TYPE DEFAULT_SEQUENCE_TYPE = SEQUENCE_TYPE.CACHED;

  public static YTSequence createSequence(SEQUENCE_TYPE sequenceType, YTDocument document) {
    return switch (sequenceType) {
      case ORDERED -> new YTSequenceOrdered(document);
      case CACHED -> new YTSequenceCached(document);
    };
  }

  public static YTSequence createSequence(
      SEQUENCE_TYPE sequenceType, YTSequence.CreateParams params, String name) {
    return switch (sequenceType) {
      case ORDERED -> new YTSequenceOrdered(params, name);
      case CACHED -> new YTSequenceCached(params, name);
    };
  }

  public static SEQUENCE_TYPE getSequenceTyeFromString(String typeAsString) {
    return SEQUENCE_TYPE.valueOf(typeAsString);
  }

  public static YTSequence createSequence(YTDocument document) {
    SEQUENCE_TYPE sequenceType = YTSequence.getSequenceType(document);
    return createSequence(sequenceType, document);
  }
}
