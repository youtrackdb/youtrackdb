package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.core.collate.Collate;
import com.jetbrains.youtrack.db.internal.core.index.PropertyMapIndexDefinition.INDEX_BY;

public class IndexConfigProperty {

  protected final String name;
  protected final PropertyType type;
  protected final PropertyType linkedType;
  protected final Collate collate;
  protected final INDEX_BY index_by;

  public IndexConfigProperty(
      String name, PropertyType type, PropertyType linkedType, Collate collate, INDEX_BY index_by) {
    this.name = name;
    this.type = type;
    this.linkedType = linkedType;
    this.collate = collate;
    this.index_by = index_by;
  }

  public Collate getCollate() {
    return collate;
  }

  public PropertyType getLinkedType() {
    return linkedType;
  }

  public String getName() {
    return name;
  }

  public PropertyType getType() {
    return type;
  }

  public INDEX_BY getIndexBy() {
    return index_by;
  }

  public IndexConfigProperty copy() {
    return new IndexConfigProperty(
        this.name, this.type, this.linkedType, this.collate, this.index_by);
  }
}
