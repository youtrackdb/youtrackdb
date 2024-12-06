package com.orientechnologies.lucene.engine;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Property;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OLuceneClassIndexContext {

  protected final IndexDefinition definition;
  protected final String name;
  protected final boolean automatic;
  protected final EntityImpl metadata;
  protected final Map<String, Boolean> fieldsToStore = new HashMap<String, Boolean>();
  protected final SchemaClass indexClass;

  public OLuceneClassIndexContext(
      Schema schema,
      IndexDefinition definition,
      String name,
      boolean automatic,
      EntityImpl metadata) {
    this.definition = definition;
    this.name = name;
    this.automatic = automatic;
    this.metadata = metadata;

    LogManager.instance().info(this, "index definition:: " + definition);

    indexClass = schema.getClass(definition.getClassName());

    updateFieldToStore(definition);
  }

  private void updateFieldToStore(IndexDefinition indexDefinition) {

    List<String> fields = indexDefinition.getFields();

    for (String field : fields) {
      Property property = indexClass.getProperty(field);

      if (property.getType().isEmbedded() && property.getLinkedType() != null) {
        fieldsToStore.put(field, true);
      } else {
        fieldsToStore.put(field, false);
      }
    }
  }

  public boolean isFieldToStore(String field) {
    if (fieldsToStore.containsKey(field)) {
      return fieldsToStore.get(field);
    }
    return false;
  }
}
