package com.jetbrains.youtrack.db.api.schema;

import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class IndexDefinition {

  @Nonnull
  private final String name;
  @Nonnull
  private final String className;
  @Nonnull
  private final Collection<String> properties;
  @Nonnull
  private final INDEX_TYPE type;
  private final boolean nullValuesIgnored;
  @Nullable
  private final String collate;
  @Nonnull
  private final Map<String, ?> metadata;

  public IndexDefinition(@Nonnull String name,
      @Nonnull String className,
      @Nonnull Collection<String> properties,
      @Nonnull INDEX_TYPE type,
      boolean nullValuesIgnored,
      @Nullable String collate,
      @Nonnull Map<String, ?> metadata) {
    this.name = name;
    this.className = className;
    this.properties = properties;
    this.type = type;
    this.nullValuesIgnored = nullValuesIgnored;
    this.collate = collate;
    this.metadata = metadata;
  }

  @Nonnull
  public String getName() {
    return name;
  }

  @Nonnull
  public String getClassName() {
    return className;
  }

  @Nonnull
  public Collection<String> getProperties() {
    return properties;
  }

  @Nonnull
  public INDEX_TYPE getType() {
    return type;
  }

  public boolean isNullValuesIgnored() {
    return nullValuesIgnored;
  }

  @Nullable
  public String getCollate() {
    return collate;
  }

  @Nonnull
  public Map<String, ?> getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    var that = (IndexDefinition) o;
    return nullValuesIgnored == that.nullValuesIgnored && name.equals(that.name)
        && className.equals(
        that.className) && properties.equals(that.properties) && type == that.type
        && Objects.equals(collate, that.collate) && metadata.equals(that.metadata);
  }

  @Override
  public int hashCode() {
    var result = name.hashCode();
    result = 31 * result + className.hashCode();
    result = 31 * result + properties.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + Boolean.hashCode(nullValuesIgnored);
    result = 31 * result + Objects.hashCode(collate);
    result = 31 * result + metadata.hashCode();
    return result;
  }
}
