package com.orientechnologies.core.metadata.schema;

import com.orientechnologies.core.collate.OCollate;
import com.orientechnologies.core.index.OPropertyMapIndexDefinition.INDEX_BY;
import java.util.ArrayList;
import java.util.List;

public class OViewIndexConfig {

  protected final String type;
  protected final String engine;

  protected List<OIndexConfigProperty> props = new ArrayList<>();

  OViewIndexConfig(String type, String engine) {
    this.type = type;
    this.engine = engine;
  }

  public void addProperty(
      String name, YTType type, YTType linkedType, OCollate collate, INDEX_BY indexBy) {
    this.props.add(new OIndexConfigProperty(name, type, linkedType, collate, indexBy));
  }

  public List<OIndexConfigProperty> getProperties() {
    return props;
  }

  public String getType() {
    return type;
  }

  public String getEngine() {
    return engine;
  }
}
