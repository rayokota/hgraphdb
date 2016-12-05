package io.hgraphdb.process.strategy.optimization;

import io.hgraphdb.process.step.sideEffect.HBaseVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HBaseVertexStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final HBaseVertexStepStrategy INSTANCE = new HBaseVertexStepStrategy();

    private HBaseVertexStepStrategy() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final VertexStep originalVertexStep : TraversalHelper.getStepsOfClass(VertexStep.class, traversal)) {
            final HBaseVertexStep<?> hbaseVertexStep = new HBaseVertexStep<>(originalVertexStep);
            TraversalHelper.replaceStep(originalVertexStep, hbaseVertexStep, traversal);
            Step<?, ?> currentStep = hbaseVertexStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        hbaseVertexStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static HBaseVertexStepStrategy instance() {
        return INSTANCE;
    }

}
