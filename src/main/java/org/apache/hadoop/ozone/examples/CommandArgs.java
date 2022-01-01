package org.apache.hadoop.ozone.examples;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CommandArgs {
    @Parameter(names = "-group", description = "Comma-separated list of Client ip",
            splitter = CommaParameterSplitter.class)
    public List<String> groups = new ArrayList<>(Collections.singletonList("127.0.0.1"));

    @Parameter(names = "-volume")
    public String volume = "test";

    @Parameter(names = "-bucket")
    public String bucket = "test";

    @Parameter(names = "-num")
    public int fileNum = 10;

    @Parameter(names = "-chunkSize")
    public int chunkSize = 512;

    @Parameter(names = "-fileSize")
    public int fileSize = 10 * 1024 * 1024;

    @Parameter(names = "-checksum")
    public boolean disableChecksum = false;
}
