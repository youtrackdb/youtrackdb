/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.operator;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.OIndexSearchResult;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterItemField;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OIndexReuseType;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OQueryTargetOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.ParseException;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.query.OLuceneKeyAndMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

public class OLuceneTextOperator extends OQueryTargetOperator {

  public static final String MEMORY_INDEX = "_memoryIndex";

  public OLuceneTextOperator() {
    this("LUCENE", 5, false);
  }

  public OLuceneTextOperator(String iKeyword, int iPrecedence, boolean iLogical) {
    super(iKeyword, iPrecedence, iLogical);
  }

  protected static YTDatabaseSessionInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return OIndexReuseType.INDEX_OPERATOR;
  }

  @Override
  public OIndexSearchResult getOIndexSearchResult(
      YTClass iSchemaClass,
      OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults,
      CommandContext context) {

    // FIXME questo non trova l'indice se l'ordine e' errato
    return OLuceneOperatorUtil.buildOIndexSearchResult(
        iSchemaClass, iCondition, iIndexSearchResults, context);
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> executeIndexQuery(
      CommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    if (!index.getType().toLowerCase().contains("fulltext")) {
      return null;
    }
    if (index.getAlgorithm() == null || !index.getAlgorithm().toLowerCase().contains("lucene")) {
      return null;
    }

    return index
        .getInternal()
        .getRids(iContext.getDatabase(),
            new OLuceneKeyAndMetadata(
                new OLuceneCompositeKey(keyParams).setContext(iContext), Collections.emptyMap()))
        .map((rid) -> new ORawPair<>(new OLuceneCompositeKey(keyParams).setContext(iContext), rid));
  }

  @Override
  public YTRID getBeginRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public YTRID getEndRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public boolean canBeMerged() {
    return false;
  }

  @Override
  public Object evaluateRecord(
      YTIdentifiable iRecord,
      EntityImpl iCurrentResult,
      OSQLFilterCondition iCondition,
      Object iLeft,
      Object iRight,
      CommandContext iContext,
      final ODocumentSerializer serializer) {

    OLuceneFullTextIndex index = involvedIndex(iContext.getDatabase(), iRecord, iCurrentResult,
        iCondition, iLeft,
        iRight);
    if (index == null) {
      return false;
    }

    MemoryIndex memoryIndex = (MemoryIndex) iContext.getVariable(MEMORY_INDEX);
    if (memoryIndex == null) {
      memoryIndex = new MemoryIndex();
      iContext.setVariable(MEMORY_INDEX, memoryIndex);
    }
    memoryIndex.reset();

    try {
      // In case of collection field evaluate the query with every item until matched

      if (iLeft instanceof List && index.isCollectionIndex()) {
        return matchCollectionIndex(iContext.getDatabase(), (List) iLeft, iRight, index,
            memoryIndex);
      } else {
        return matchField(iContext.getDatabase(), iLeft, iRight, index, memoryIndex);
      }

    } catch (ParseException e) {
      LogManager.instance().error(this, "error occurred while building query", e);

    } catch (IOException e) {
      LogManager.instance().error(this, "error occurred while building memory index", e);
    }
    return null;
  }

  private boolean matchField(
      YTDatabaseSessionInternal session, Object iLeft, Object iRight, OLuceneFullTextIndex index,
      MemoryIndex memoryIndex)
      throws IOException, ParseException {
    for (IndexableField field : index.buildDocument(session, iLeft).getFields()) {
      memoryIndex.addField(field, index.indexAnalyzer());
    }
    return memoryIndex.search(index.buildQuery(iRight)) > 0.0f;
  }

  private boolean matchCollectionIndex(
      YTDatabaseSessionInternal session, List iLeft, Object iRight, OLuceneFullTextIndex index,
      MemoryIndex memoryIndex)
      throws IOException, ParseException {
    boolean match = false;
    List<Object> collections = transformInput(iLeft, iRight, index, memoryIndex);
    for (Object collection : collections) {
      memoryIndex.reset();
      match = match || matchField(session, collection, iRight, index, memoryIndex);
      if (match) {
        break;
      }
    }
    return match;
  }

  private List<Object> transformInput(
      List iLeft, Object iRight, OLuceneFullTextIndex index, MemoryIndex memoryIndex) {

    Integer collectionIndex = getCollectionIndex(iLeft);
    if (collectionIndex == -1) {
      // collection not found;
      return iLeft;
    }
    if (collectionIndex > 1) {
      throw new UnsupportedOperationException("Index of collection cannot be > 1");
    }
    // otherwise the input is [val,[]] or [[],val]
    Collection collection = (Collection) iLeft.get(collectionIndex);
    if (iLeft.size() == 1) {
      return new ArrayList<Object>(collection);
    }
    List<Object> transformed = new ArrayList<Object>(collection.size());
    for (Object o : collection) {
      List<Object> objects = new ArrayList<Object>();
      //  [[],val]
      if (collectionIndex == 0) {
        objects.add(o);
        objects.add(iLeft.get(1));
        //  [val,[]]
      } else {
        objects.add(iLeft.get(0));
        objects.add(o);
      }
      transformed.add(objects);
    }
    return transformed;
  }

  private Integer getCollectionIndex(List iLeft) {
    int i = 0;
    for (Object o : iLeft) {
      if (o instanceof Collection) {
        return i;
      }
      i++;
    }
    return -1;
  }

  protected OLuceneFullTextIndex involvedIndex(
      YTDatabaseSessionInternal session, YTIdentifiable iRecord,
      EntityImpl iCurrentResult,
      OSQLFilterCondition iCondition,
      Object iLeft,
      Object iRight) {

    try {
      EntityImpl doc = iRecord.getRecord();
      if (doc.getClassName() != null) {
        YTClass cls = getDatabase().getMetadata().getSchema().getClass(doc.getClassName());

        if (isChained(iCondition.getLeft())) {

          OSQLFilterItemField chained = (OSQLFilterItemField) iCondition.getLeft();

          OSQLFilterItemField.FieldChain fieldChain = chained.getFieldChain();
          YTClass oClass = cls;
          for (int i = 0; i < fieldChain.getItemCount() - 1; i++) {
            oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
          }
          if (oClass != null) {
            cls = oClass;
          }
        }
        Set<OIndex> classInvolvedIndexes = cls.getInvolvedIndexes(session, fields(iCondition));
        OLuceneFullTextIndex idx = null;
        for (OIndex classInvolvedIndex : classInvolvedIndexes) {

          if (classInvolvedIndex.getInternal() instanceof OLuceneFullTextIndex) {
            idx = (OLuceneFullTextIndex) classInvolvedIndex.getInternal();
            break;
          }
        }
        return idx;
      } else {
        return null;
      }
    } catch (YTRecordNotFoundException rnf) {
      return null;
    }
  }

  private boolean isChained(Object left) {
    if (left instanceof OSQLFilterItemField field) {
      return field.isFieldChain();
    }
    return false;
  }

  // returns a list of field names
  protected Collection<String> fields(OSQLFilterCondition iCondition) {

    Object left = iCondition.getLeft();

    if (left instanceof String fName) {
      return List.of(fName);
    }
    if (left instanceof Collection) {
      Collection<OSQLFilterItemField> f = (Collection<OSQLFilterItemField>) left;

      List<String> fields = new ArrayList<String>();
      for (OSQLFilterItemField field : f) {
        fields.add(field.toString());
      }
      return fields;
    }
    if (left instanceof OSQLFilterItemField fName) {

      if (fName.isFieldChain()) {
        int itemCount = fName.getFieldChain().getItemCount();
        return Collections.singletonList(fName.getFieldChain().getItemName(itemCount - 1));
      } else {
        return Collections.singletonList(fName.toString());
      }
    }
    return Collections.emptyList();
  }
}
