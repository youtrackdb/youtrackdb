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
package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransaction;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges.OPERATION;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fast index for full-text searches.
 */
@Deprecated
public class IndexFullText extends IndexMultiValues {

  private static final String CONFIG_STOP_WORDS = "stopWords";
  private static final String CONFIG_SEPARATOR_CHARS = "separatorChars";
  private static final String CONFIG_IGNORE_CHARS = "ignoreChars";
  private static final String CONFIG_INDEX_RADIX = "indexRadix";
  private static final String CONFIG_MIN_WORD_LEN = "minWordLength";
  private static final boolean DEF_INDEX_RADIX = true;
  private static final String DEF_SEPARATOR_CHARS = " \r\n\t:;,.|+*/\\=!?[]()";
  private static final String DEF_IGNORE_CHARS = "'\"";
  private static final String DEF_STOP_WORDS =
      "the in a at as and or for his her "
          + "him this that what which while "
          + "up with be was were is";
  private boolean indexRadix;
  private String separatorChars;
  private String ignoreChars;
  private int minWordLength;

  private Set<String> stopWords;

  public IndexFullText(IndexMetadata im, final Storage storage) {
    super(im, storage);
    acquireExclusiveLock();
    try {
      config();
      configWithMetadata(im.getMetadata());
    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Indexes a value and save the index. Splits the value in single words and index each one. Save
   * of the index is responsibility of the caller.
   */
  @Override
  public IndexFullText put(DatabaseSessionInternal session, Object key,
      final Identifiable value) {
    if (key == null) {
      return this;
    }
    final RecordId rid = (RecordId) value.getIdentity();

    if (!rid.isValid()) {
      if (value instanceof DBRecord) {
        // EARLY SAVE IT
        ((DBRecord) value).save();
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }

    key = getCollatingValue(key);

    final Set<String> words = splitIntoWords(key.toString());

    FrontendTransaction singleTx = session.getTransaction();
    for (String word : words) {
      singleTx.addIndexEntry(
          this, super.getName(), FrontendTransactionIndexChanges.OPERATION.PUT, word, value);
    }

    return this;
  }

  /**
   * Splits passed in key on several words and remove records with keys equals to any item of split
   * result and values equals to passed in value.
   *
   * @param session
   * @param key     Key to remove.
   * @param rid     Value to remove.
   * @return <code>true</code> if at least one record is removed.
   */
  @Override
  public boolean remove(DatabaseSessionInternal session, Object key, final Identifiable rid) {
    if (key == null) {
      return false;
    }
    key = getCollatingValue(key);

    final Set<String> words = splitIntoWords(key.toString());
    for (final String word : words) {
      session.getTransaction().addIndexEntry(this, super.getName(), OPERATION.REMOVE, word, rid);
    }

    return true;
  }

  @Override
  public IndexMultiValues create(
      DatabaseSessionInternal session, IndexMetadata metadata, boolean rebuild,
      ProgressListener progressListener) {
    if (metadata.getIndexDefinition().getFields().size() > 1) {
      throw new IndexException(getType() + " indexes cannot be used as composite ones.");
    }
    super.create(session, metadata, rebuild, progressListener);
    return this;
  }

  @Override
  public EntityImpl updateConfiguration(DatabaseSessionInternal session) {
    EntityImpl entity = super.updateConfiguration(session);
    entity.field(CONFIG_SEPARATOR_CHARS, separatorChars);
    entity.field(CONFIG_IGNORE_CHARS, ignoreChars);
    entity.field(CONFIG_STOP_WORDS, stopWords);
    entity.field(CONFIG_MIN_WORD_LEN, minWordLength);
    entity.field(CONFIG_INDEX_RADIX, indexRadix);

    return entity;
  }

  public boolean canBeUsedInEqualityOperators() {
    return false;
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  private void configWithMetadata(Map<String, ?> metadata) {
    if (metadata != null) {
      if (metadata.containsKey(CONFIG_IGNORE_CHARS)) {
        ignoreChars = metadata.get(CONFIG_IGNORE_CHARS).toString();
      }

      if (metadata.containsKey(CONFIG_INDEX_RADIX)) {
        indexRadix = (Boolean) metadata.get(CONFIG_INDEX_RADIX);
      }

      if (metadata.containsKey(CONFIG_SEPARATOR_CHARS)) {
        separatorChars = metadata.get(CONFIG_SEPARATOR_CHARS).toString();
      }

      if (metadata.containsKey(CONFIG_MIN_WORD_LEN)) {
        minWordLength = (Integer) metadata.get(CONFIG_MIN_WORD_LEN);
      }

      if (metadata.containsKey(CONFIG_STOP_WORDS)) {
        //noinspection unchecked
        stopWords = new HashSet<>((Set<String>) metadata.get(CONFIG_STOP_WORDS));
      }
    }
  }

  private void config() {
    ignoreChars = DEF_IGNORE_CHARS;
    indexRadix = DEF_INDEX_RADIX;
    separatorChars = DEF_SEPARATOR_CHARS;
    minWordLength = 3;
    stopWords = new HashSet<>(StringSerializerHelper.split(DEF_STOP_WORDS, ' '));
  }

  private Set<String> splitIntoWords(final String iKey) {
    final Set<String> result = new HashSet<>();

    final List<String> words = new ArrayList<>();
    StringSerializerHelper.split(words, iKey, 0, -1, separatorChars);

    final StringBuilder buffer = new StringBuilder(64);
    // FOREACH WORD CREATE THE LINK TO THE CURRENT ENTITY

    char c;
    boolean ignore;
    for (String word : words) {
      buffer.setLength(0);

      for (int i = 0; i < word.length(); ++i) {
        c = word.charAt(i);
        ignore = false;
        for (int k = 0; k < ignoreChars.length(); ++k) {
          if (c == ignoreChars.charAt(k)) {
            ignore = true;
            break;
          }
        }

        if (!ignore) {
          buffer.append(c);
        }
      }

      int length = buffer.length();

      while (length >= minWordLength) {
        buffer.setLength(length);
        word = buffer.toString();

        // CHECK IF IT'S A STOP WORD
        if (!stopWords.contains(word))
        // ADD THE WORD TO THE RESULT SET
        {
          result.add(word);
        }

        if (indexRadix) {
          length--;
        } else {
          break;
        }
      }
    }

    return result;
  }
}
