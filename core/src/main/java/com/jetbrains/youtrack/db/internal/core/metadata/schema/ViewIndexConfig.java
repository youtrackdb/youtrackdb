package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.core.collate.Collate;
import com.jetbrains.youtrack.db.internal.core.index.PropertyMapIndexDefinition.INDEX_BY;
import java.util.ArrayList;
import java.util.List;

public class ViewIndexConfig {

  protected final String type;
  protected final String engine;

  protected List<IndexConfigProperty> props = new ArrayList<>();

  ViewIndexConfig(String type, String engine) {
    this.type = type;
    this.engine = engine;
  }

  public void addProperty(
      String name, PropertyType type, PropertyType linkedType, Collate collate, INDEX_BY indexBy) {
    this.props.add(new IndexConfigProperty(name, type, linkedType, collate, indexBy));
  }

  public List<IndexConfigProperty> getProperties() {
    return props;
  }

  public String getType() {
    return type;
  }

  public String getEngine() {
    return engine;
  }
}
