package io.hgraphdb;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class CustomSuite extends AbstractGremlinSuite {

    public CustomSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder,
                new Class<?>[]{
                        CustomTest.class,
                },
                null,
                false,
                TraversalEngine.Type.STANDARD);
    }

}