package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.core.collate.OCollate;
import com.jetbrains.youtrack.db.internal.core.index.OPropertyMapIndexDefinition.INDEX_BY;

public class OIndexConfigProperty {

  protected final String name;
  protected final YTType type;
  protected final YTType linkedType;
  protected final OCollate collate;
  protected final INDEX_BY index_by;

  public OIndexConfigProperty(
      String name, YTType type, YTType linkedType, OCollate collate, INDEX_BY index_by) {
    this.name = name;
    this.type = type;
    this.linkedType = linkedType;
    this.collate = collate;
    this.index_by = index_by;
  }

  public OCollate getCollate() {
    return collate;
  }

  public YTType getLinkedType() {
    return linkedType;
  }

  public String getName() {
    return name;
  }

  public YTType getType() {
    return type;
  }

  public INDEX_BY getIndexBy() {
    return index_by;
  }

  public OIndexConfigProperty copy() {
    return new OIndexConfigProperty(
        this.name, this.type, this.linkedType, this.collate, this.index_by);
  }
}
