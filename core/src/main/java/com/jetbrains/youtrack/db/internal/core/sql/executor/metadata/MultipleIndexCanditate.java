package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.IndexFinder.Operation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MultipleIndexCanditate implements IndexCandidate {

  public final List<IndexCandidate> canditates = new ArrayList<IndexCandidate>();

  public MultipleIndexCanditate() {
  }

  private MultipleIndexCanditate(Collection<IndexCandidate> canditates) {
    this.canditates.addAll(canditates);
  }

  public void addCanditate(IndexCandidate canditate) {
    this.canditates.add(canditate);
  }

  public List<IndexCandidate> getCanditates() {
    return canditates;
  }

  @Override
  public String getName() {
    String name = "";
    for (IndexCandidate indexCandidate : canditates) {
      name = indexCandidate.getName() + "|";
    }
    return name;
  }

  @Override
  public Optional<IndexCandidate> invert() {
    // TODO: when handling operator invert it
    return Optional.of(this);
  }

  @Override
  public Operation getOperation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<IndexCandidate> normalize(CommandContext ctx) {
    Collection<IndexCandidate> newCanditates = normalizeBetween(this.canditates, ctx);
    newCanditates = normalizeComposite(newCanditates, ctx);
    if (newCanditates.isEmpty()) {
      return Optional.empty();
    } else if (newCanditates.size() == 1) {
      return Optional.of(newCanditates.iterator().next());
    } else {
      return Optional.of(new MultipleIndexCanditate(newCanditates));
    }
  }

  private Collection<IndexCandidate> normalizeBetween(
      List<IndexCandidate> canditates, CommandContext ctx) {
    List<IndexCandidate> newCanditates = new ArrayList<>();
    for (int i = 0; i < canditates.size(); i++) {
      boolean matched = false;
      IndexCandidate canditate = canditates.get(i);
      List<Property> properties = canditate.properties();
      for (int z = canditates.size() - 1; z > i; z--) {
        IndexCandidate lastCandidate = canditates.get(z);
        List<Property> lastProperties = lastCandidate.properties();
        if (properties.size() == 1
            && lastProperties.size() == 1
            && properties.get(0).getName() == lastProperties.get(0).getName()) {
          if (canditate.getOperation().isRange() || lastCandidate.getOperation().isRange()) {
            newCanditates.add(new RangeIndexCanditate(canditate.getName(), properties.get(0)));
            canditates.remove(z);
            if (z != canditates.size()) {
              z++; // Increase so it does not decrease next iteration
            }
            matched = true;
          }
        }
      }
      if (!matched) {
        newCanditates.add(canditate);
      }
    }
    return newCanditates;
  }

  private Collection<IndexCandidate> normalizeComposite(
      Collection<IndexCandidate> canditates, CommandContext ctx) {
    List<Property> propeties = properties();
    Map<String, IndexCandidate> newCanditates = new HashMap<>();
    for (IndexCandidate cand : canditates) {
      if (!newCanditates.containsKey(cand.getName())) {
        Index index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(cand.getName());
        List<Property> foundProps = new ArrayList<>();
        for (String field : index.getDefinition().getFields()) {
          boolean found = false;
          for (Property property : propeties) {
            if (property.getName().equals(field)) {
              found = true;
              foundProps.add(property);
              break;
            }
          }
          if (!found) {
            break;
          }
        }
        if (foundProps.size() == 1) {
          newCanditates.put(index.getName(), cand);
        } else if (!foundProps.isEmpty()) {
          newCanditates.put(
              index.getName(),
              new IndexCandidateComposite(index.getName(), cand.getOperation(), foundProps));
        }
      }
    }
    return newCanditates.values();
  }

  @Override
  public List<Property> properties() {
    List<Property> props = new ArrayList<>();
    for (IndexCandidate cand : this.canditates) {
      props.addAll(cand.properties());
    }
    return props;
  }
}
