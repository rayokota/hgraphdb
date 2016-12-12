package io.hgraphdb.models;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphException;
import io.hgraphdb.ValueUtils;
import io.hgraphdb.mutators.PropertyIncrementer;
import io.hgraphdb.mutators.PropertyRemover;
import io.hgraphdb.mutators.PropertyWriter;
import io.hgraphdb.readers.ElementReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class ElementModel extends BaseModel {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElementModel.class);

    public ElementModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public abstract ElementReader getReader();

    /**
     * Load the element from the backing table.
     *
     * @param element The element
     */
    @SuppressWarnings("unchecked")
    public void load(Element element) {
        LOGGER.trace("Executing Get, type: {}, id: {}", getClass().getSimpleName(), element.id());

        Get get = new Get(ValueUtils.serializeWithSalt(element.id()));

        try {
            Result result = table.get(get);
            getReader().load(element, result);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    /**
     * Delete the property entry from property table.
     *
     * @param element The element
     * @param key     The property key
     */
    public PropertyRemover clearProperty(Element element, String key) {
        return new PropertyRemover(graph, element, key);
    }

    /**
     * Write the given property to the property table.
     *
     * @param element The element
     * @param key     The property key
     * @param value   The property value
     */
    public PropertyWriter writeProperty(Element element, String key, Object value) {
        return new PropertyWriter(graph, element, key, value);
    }

    /**
     * Increment the given property in the property table.
     *
     * @param element The element
     * @param key     The property key
     * @param value   The amount to increment
     */
    public PropertyIncrementer incrementProperty(Element element, String key, long value) {
        return new PropertyIncrementer(graph, element, key, value);
    }

    protected Scan getPropertyScan(String label) {
        Scan scan = new Scan();
        SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                Constants.LABEL_BYTES, CompareFilter.CompareOp.EQUAL, new BinaryComparator(ValueUtils.serialize(label)));
        valueFilter.setFilterIfMissing(true);
        scan.setFilter(valueFilter);
        return scan;
    }

    protected Scan getPropertyScan(String label, byte[] key, byte[] val) {
        Scan scan = new Scan();
        SingleColumnValueFilter labelFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                Constants.LABEL_BYTES, CompareFilter.CompareOp.EQUAL, new BinaryComparator(ValueUtils.serialize(label)));
        labelFilter.setFilterIfMissing(true);
        SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                key, CompareFilter.CompareOp.EQUAL, new BinaryComparator(val));
        valueFilter.setFilterIfMissing(true);
        FilterList filterList = new FilterList(labelFilter, valueFilter);
        scan.setFilter(filterList);
        return scan;
    }

    protected Scan getPropertyScan(String label, byte[] key, byte[] inclusiveFromValue, byte[] exclusiveToValue) {
        Scan scan = new Scan();
        SingleColumnValueFilter labelFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                Constants.LABEL_BYTES, CompareFilter.CompareOp.EQUAL, new BinaryComparator(ValueUtils.serialize(label)));
        labelFilter.setFilterIfMissing(true);
        SingleColumnValueFilter fromValueFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                key, CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(inclusiveFromValue));
        fromValueFilter.setFilterIfMissing(true);
        SingleColumnValueFilter toValueFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                key, CompareFilter.CompareOp.LESS, new BinaryComparator(exclusiveToValue));
        toValueFilter.setFilterIfMissing(true);
        FilterList filterList = new FilterList(labelFilter, fromValueFilter, toValueFilter);
        scan.setFilter(filterList);
        return scan;
    }
}
