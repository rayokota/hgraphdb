package io.hgraphdb.process.strategy.optimization;

import io.hgraphdb.process.step.sideEffect.HBaseGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HBaseGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final HBaseGraphStepStrategy INSTANCE = new HBaseGraphStepStrategy();

    private HBaseGraphStepStrategy() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            final HBaseGraphStep<?, ?> neo4jGraphStep = new HBaseGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, neo4jGraphStep, traversal);
            Step<?, ?> currentStep = neo4jGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(neo4jGraphStep, hasContainer))
                            neo4jGraphStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static HBaseGraphStepStrategy instance() {
        return INSTANCE;
    }

}