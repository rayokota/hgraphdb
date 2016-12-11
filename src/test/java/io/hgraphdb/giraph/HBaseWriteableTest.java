package io.hgraphdb.giraph;

import com.google.common.collect.ImmutableMap;
import io.hgraphdb.HBaseEdge;
import io.hgraphdb.HBaseVertex;
import org.junit.Test;

import java.io.*;
import java.util.Map;

import static org.junit.Assert.*;

public class HBaseWriteableTest {

    @Test
    public void testVertexWritable() throws Exception {
        Map<String, Object> properties = ImmutableMap.of("foo", 4L, "bar", "barVal");
        HBaseVertex v = new HBaseVertex(null, 1, "mylabel", 2L, 3L, properties);
        VertexWritable writable = new VertexWritable(v);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(baos);
        writable.write(output);
        byte[] bytes = baos.toByteArray();
        writable.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
        HBaseVertex v2 = writable.get();
        assertEquals(v, v2);
    }

    @Test
    public void testEdgeWritable() throws Exception {
        Map<String, Object> properties = ImmutableMap.of("foo", 4L, "bar", "barVal");
        HBaseEdge e = new HBaseEdge(null, 1, "mylabel", 2L, 3L, properties, new HBaseVertex(null, 10), new HBaseVertex(null, 20));
        EdgeWritable writable = new EdgeWritable(e);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(baos);
        writable.write(output);
        byte[] bytes = baos.toByteArray();
        writable.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
        HBaseEdge e2 = writable.get();
        assertEquals(e, e2);
    }
}
