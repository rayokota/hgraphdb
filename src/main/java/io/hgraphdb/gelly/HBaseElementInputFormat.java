package io.hgraphdb.gelly;

import io.hgraphdb.Constants;
import io.hgraphdb.ElementType;
import io.hgraphdb.HBaseElement;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.mock.MockHTable;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public abstract class HBaseElementInputFormat<T extends Tuple> extends TableInputFormat<T> {

    private static final long serialVersionUID = 6633419799225743575L;
    
    protected final HBaseGraphConfiguration hConf;
    protected final ElementType elementType;
    protected final String propertyName;
    protected transient HBaseGraph graph;

    public HBaseElementInputFormat(HBaseGraphConfiguration hConf, ElementType elementType, String propertyName) {
        this.hConf = hConf;
        this.elementType = elementType;
        this.propertyName = propertyName;
    }

    public HBaseGraphConfiguration getConfiguration() {
        return hConf;
    }

    public boolean isMock() {
        return HBaseGraphConfiguration.InstanceType.MOCK.toString().equals(
                getConfiguration().getProperty(HBaseGraphConfiguration.Keys.INSTANCE_TYPE));
    }

    public ElementType getElementType() {
        return elementType;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public HBaseGraph getGraph() {
        return graph;
    }

    public Table getTable() {
        return elementType == ElementType.EDGE
                ? getGraph().getEdgeModel().getTable()
                : getGraph().getVertexModel().getTable();
    }

    @Override
    public String getTableName() {
        return getTable().getName().getNameAsString();
    }

    @Override
    protected Scan getScanner() {
        return new Scan();
    }

    @Override
    public void configure(Configuration parameters) {
        try {
            graph = new HBaseGraph(hConf);
            Table t = getTable();
            table = isMock() ? ((MockHTable) t).asHTable() : (HTable) t;
            if (table != null) {
                scan = getScanner();
            }
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    protected abstract T mapResultToTuple(Result r);

    @SuppressWarnings("unchecked")
    static <V> V property(HBaseElement element, String propertyName) {
        if (Constants.ELEMENT_ID.equals(propertyName)) {
            return (V) element.id();
        } else if (Constants.LABEL.equals(propertyName)) {
            return (V) element.label();
        } else if (element.hasProperty(propertyName)) {
            return (V) element.property(propertyName).value();
        } else {
            return null;
        }
    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();
        if (graph != null) {
            graph.close();
        }
    }
}
