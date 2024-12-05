package com.orientechnologies.lucene.engine;

import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OLuceneClassIndexContext {

  protected final OIndexDefinition definition;
  protected final String name;
  protected final boolean automatic;
  protected final EntityImpl metadata;
  protected final Map<String, Boolean> fieldsToStore = new HashMap<String, Boolean>();
  protected final YTClass indexClass;

  public OLuceneClassIndexContext(
      YTSchema schema,
      OIndexDefinition definition,
      String name,
      boolean automatic,
      EntityImpl metadata) {
    this.definition = definition;
    this.name = name;
    this.automatic = automatic;
    this.metadata = metadata;

    OLogManager.instance().info(this, "index definition:: " + definition);

    indexClass = schema.getClass(definition.getClassName());

    updateFieldToStore(definition);
  }

  private void updateFieldToStore(OIndexDefinition indexDefinition) {

    List<String> fields = indexDefinition.getFields();

    for (String field : fields) {
      YTProperty property = indexClass.getProperty(field);

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
