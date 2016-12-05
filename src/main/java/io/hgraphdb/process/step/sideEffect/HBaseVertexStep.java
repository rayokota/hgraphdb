package io.hgraphdb.process.step.sideEffect;

import io.hgraphdb.ElementType;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseVertex;
import io.hgraphdb.OperationType;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public final class HBaseVertexStep<E extends Element> extends VertexStep<E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public HBaseVertexStep(final VertexStep<E> originalVertexStep) {
        super(originalVertexStep.getTraversal(), originalVertexStep.getReturnClass(), originalVertexStep.getDirection(), originalVertexStep.getEdgeLabels());
        originalVertexStep.getLabels().forEach(this::addLabel);
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        return Vertex.class.isAssignableFrom(getReturnClass()) ?
                (Iterator<E>) traverser.get().vertices(getDirection(), getEdgeLabels()) :
                (Iterator<E>) lookupEdges(traverser, this.hasContainers);
    }

    private Iterator<Edge> lookupEdges(final Traverser.Admin<Vertex> traverser, final List<HasContainer> hasContainers) {
        final HBaseGraph graph = (HBaseGraph) this.getTraversal().getGraph().get();
        if (getEdgeLabels().length == 1) {
            final String label = getEdgeLabels()[0];
            // find an edge by label and key/value
            for (final HasContainer hasContainer : hasContainers) {
                if (Compare.eq == hasContainer.getBiPredicate() && !hasContainer.getKey().equals(T.label.getAccessor())) {
                    if (graph.hasIndex(OperationType.READ, ElementType.EDGE, label, hasContainer.getKey())) {
                        return IteratorUtils.stream(((HBaseVertex) traverser.get()).edges(getDirection(), label, hasContainer.getKey(), hasContainer.getValue()))
                                .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
                    }
                }
            }
        }

        // linear scan
        return IteratorUtils.filter(traverser.get().edges(getDirection(), getEdgeLabels()),
                edge -> HasContainer.testAll(edge, hasContainers));
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return StringFactory.stepString(this, getDirection(), Arrays.asList(getEdgeLabels()), getReturnClass().getSimpleName().toLowerCase(), this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
