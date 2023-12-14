package com.alibaba.datax.example.streamreader;

import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.Test;

/**
 * {@code Author} FuYouJ
 * {@code Date} 2023/8/14 20:16
 */

public class StreamReader2StreamWriterTest {
    @Test
    public void testStreamReader2StreamWriter() {
        String path = "/stream2stream.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

// com.alibaba.datax.core.transport.record.DefaultRecord
    @Test
    public void testPGReader2StreamWriter() {
        String path = "/pg2sr.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }


    @Test
    public void testMysqlReader2StreamWriter() {
        String path = "/mysql2sr.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }


    @Test
    public void testListingMysqlReader2StreamWriter() {
        String path = "/listing.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }

    @Test
    public void testYYPGReader2StreamWriter() {
        String path = "/yypg2sr.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }
}
