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
package com.jetbrains.youtrack.db.internal.core.sql.filter;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.parser.BaseParser;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.BinaryField;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.BytesContainer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerBinary;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.SQLMethodField;
import java.util.Set;

/**
 * Represent an object field as value in the query condition.
 */
public class SQLFilterItemField extends SQLFilterItemAbstract {

  protected Set<String> preLoadedFields;
  protected String[] preLoadedFieldsArray;
  protected String name;
  protected Collate collate;
  private boolean collatePreset = false;
  private String stringValue;

  /**
   * Represents filter item as chain of fields. Provide interface to work with this chain like with
   * sequence of field names.
   */
  public class FieldChain {

    private FieldChain() {
    }

    public String getItemName(int fieldIndex) {
      if (fieldIndex == 0) {
        return name;
      } else {
        return operationsChain.get(fieldIndex - 1).getValue()[0].toString();
      }
    }

    public int getItemCount() {
      if (operationsChain == null) {
        return 1;
      } else {
        return operationsChain.size() + 1;
      }
    }

    /**
     * Field chain is considered as long chain if it contains more than one item.
     *
     * @return true if this chain is long and false in another case.
     */
    public boolean isLong() {
      return operationsChain != null && operationsChain.size() > 0;
    }

    public boolean belongsTo(SQLFilterItemField filterItemField) {
      return SQLFilterItemField.this == filterItemField;
    }
  }

  public SQLFilterItemField(DatabaseSessionInternal db, final String iName,
      final SchemaClass iClass) {
    this.name = IOUtils.getStringContent(iName);
    collate = getCollateForField(db, iClass, name);
    if (iClass != null) {
      collatePreset = true;
    }
  }

  public SQLFilterItemField(
      DatabaseSessionInternal session, final BaseParser iQueryToParse, final String iName,
      final SchemaClass iClass) {
    super(session, iQueryToParse, iName);
    collate = getCollateForField(session, iClass, iName);
    if (iClass != null) {
      collatePreset = true;
    }
  }

  public Object getValue(
      final Identifiable iRecord, final Object iCurrentResult, final CommandContext iContext) {
    var session = iContext.getDatabaseSession();
    if (iRecord == null) {
      throw new CommandExecutionException(session,
          "expression item '" + name + "' cannot be resolved because current record is NULL");
    }

    if (preLoadedFields != null && preLoadedFields.size() == 1) {
      if ("@rid".equalsIgnoreCase(preLoadedFields.iterator().next())) {
        return iRecord.getIdentity();
      }
    }

    final EntityImpl entity = iRecord.getRecord(session);

    if (preLoadedFieldsArray == null
        && preLoadedFields != null
        && !preLoadedFields.isEmpty()
        && preLoadedFields.size() < 5) {
      // TRANSFORM THE SET IN ARRAY ONLY THE FIRST TIME AND IF FIELDS ARE MORE THAN ONE, OTHERWISE
      // GO WITH THE DEFAULT BEHAVIOR
      preLoadedFieldsArray = new String[preLoadedFields.size()];
      preLoadedFields.toArray(preLoadedFieldsArray);
    }

    // UNMARSHALL THE SINGLE FIELD
    if (preLoadedFieldsArray != null && !entity.deserializeFields(preLoadedFieldsArray)) {
      return null;
    }

    final var v = stringValue == null ? entity.rawField(name) : stringValue;

    if (!collatePreset) {
      SchemaImmutableClass result = null;
      result = entity.getImmutableSchemaClass(session);
      SchemaClass schemaClass = result;
      if (schemaClass != null) {
        collate = getCollateForField(session, schemaClass, name);
      }
    }

    return transformValue(iRecord, iContext, v);
  }

  public BinaryField getBinaryField(DatabaseSessionInternal session, final Identifiable iRecord) {
    if (iRecord == null) {
      throw new CommandExecutionException(session,
          "expression item '" + name + "' cannot be resolved because current record is NULL");
    }

    if (operationsChain != null && !operationsChain.isEmpty())
    // CANNOT USE BINARY FIELDS
    {
      return null;
    }

    final EntityImpl rec = iRecord.getRecord(session);
    var encryption = EntityInternalUtils.getPropertyEncryption(rec);
    var serialized = new BytesContainer(rec.toStream());
    var version = serialized.bytes[serialized.offset++];
    var serializer = RecordSerializerBinary.INSTANCE.getSerializer(version);

    // check for embedded objects, they have invalid ID and they are serialized with class name
    SchemaImmutableClass result = null;
    if (rec != null) {
      result = rec.getImmutableSchemaClass(session);
    }
    return serializer.deserializeField(session,
        serialized,
        result,
        name,
        rec.isEmbedded(),
        session.getMetadata().getImmutableSchemaSnapshot(), encryption);
  }

  public String getRoot(DatabaseSession session) {
    return name;
  }

  public void setRoot(DatabaseSessionInternal session, final BaseParser iQueryToParse,
      final String iRoot) {
    if (isStringLiteral(iRoot)) {
      this.stringValue = IOUtils.getStringContent(iRoot);
    }
    // TODO support all the basic types
    this.name = IOUtils.getStringContent(iRoot);
  }

  private static boolean isStringLiteral(String iRoot) {
    if (iRoot.startsWith("'") && iRoot.endsWith("'")) {
      return true;
    }
    return iRoot.startsWith("\"") && iRoot.endsWith("\"");
  }

  /**
   * Check whether or not this filter item is chain of fields (e.g. "field1.field2.field3"). Return
   * true if filter item contains only field projections operators, if field item contains any other
   * projection operator the method returns false. When filter item does not contains any chain
   * operator, it is also field chain consist of one field.
   *
   * @return whether or not this filter item can be represented as chain of fields.
   */
  public boolean isFieldChain() {
    if (operationsChain == null) {
      return true;
    }

    for (var pair : operationsChain) {
      if (!pair.getKey().getMethod().getName().equals(SQLMethodField.NAME)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Creates {@code FieldChain} in case when filter item can have such representation.
   *
   * @return {@code FieldChain} representation of this filter item.
   * @throws IllegalStateException if this filter item cannot be represented as {@code FieldChain}.
   */
  public FieldChain getFieldChain() {
    if (!isFieldChain()) {
      throw new IllegalStateException("Filter item field contains not only field operators");
    }

    return new FieldChain();
  }

  public void setPreLoadedFields(final Set<String> iPrefetchedFieldList) {
    this.preLoadedFields = iPrefetchedFieldList;
  }

  public Collate getCollate() {
    return collate;
  }

  /**
   * get the collate of this expression, based on the fully evaluated field chain starting from the
   * passed object.
   *
   * @param session
   * @param object  the root element (entity?) of this field chain
   * @return the collate, null if no collate is defined
   */
  public Collate getCollate(DatabaseSessionInternal session, Object object) {
    if (collate != null || operationsChain == null || !isFieldChain()) {
      return collate;
    }
    if (!(object instanceof Identifiable)) {
      return null;
    }
    var chain = getFieldChain();
    try {
      EntityImpl lastDoc = ((Identifiable) object).getRecord(session);
      for (var i = 0; i < chain.getItemCount() - 1; i++) {
        var nextDoc = lastDoc.field(chain.getItemName(i));
        if (!(nextDoc instanceof Identifiable)) {
          return null;
        }
        lastDoc = ((Identifiable) nextDoc).getRecord(session);
      }
      SchemaImmutableClass result = null;
      if (lastDoc != null) {
        result = lastDoc.getImmutableSchemaClass(session);
      }
      SchemaClass schemaClass = result;
      if (schemaClass == null) {
        return null;
      }
      var property = schemaClass.getProperty(session,
          chain.getItemName(chain.getItemCount() - 1));
      if (property == null) {
        return null;
      }
      return property.getCollate(session);
    } catch (RecordNotFoundException rnf) {
      return null;
    }
  }
}
