package com.datastax.loader;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.loader.parser.BooleanParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * @author Michael Arndt
 */
abstract class ConfigurationLoader {
    protected static final String version = "0.0.21-SNAPSHOT";
    protected String host = null;
    protected int port = 9042;
    protected String username = null;
    protected String password = null;
    protected String truststorePath = null;
    protected String truststorePwd = null;
    protected String keystorePath = null;
    protected String keystorePwd = null;
    protected Session session = null;
    protected String cqlSchema = null;
    protected String filename = null;
    protected Locale locale = null;
    protected BooleanParser.BoolStyle boolStyle = null;
    protected String dateFormatString = null;
    protected String nullString = null;
    protected String delimiter = null;
    protected int numThreads = 5;
    protected Cluster cluster = null;
    protected ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_ONE;
    protected Character quote = null;
    protected Character escape = null;
    protected Integer maxCharsPerColumn = null;

    protected boolean processConfigFile(String fname, Map<String, String> amap)
            throws IOException {
            File cFile = new File(fname);
            if (!cFile.isFile()) {
                System.err.println("Configuration File must be a file");
                return false;
            }

            BufferedReader cReader = new BufferedReader(new FileReader(cFile));
            String line;
            while ((line = cReader.readLine()) != null) {
                String[] fields = line.trim().split("\\s+");
                if (2 != fields.length) {
                    System.err.println("Bad line in config file: " + line);
                    return false;
                }
                amap.putIfAbsent(fields[0], fields[1]);
            }
            return true;
    }
}
