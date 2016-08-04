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

import static com.datastax.loader.util.FileUtils.checkFile;

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

    protected String commonUsage() {
        return "  -configFile <filename>         File with configuration options\n" +
                "  -delim <delimiter>             Delimiter to use [,]\n" +
                "  -quote <quote>                 Quote character to use [\"]\n" +
                "  -escape <escape>               Escape character to use [\\]\n" +
                "  -dateFormat <dateFormatString> Date format [default for Locale.ENGLISH]\n" +
                "  -nullString <nullString>       String that signifies NULL [none]\n" +
                "  -port <portNumber>             CQL Port Number [9042]\n" +
                "  -user <username>               Cassandra username [none]\n" +
                "  -pw <password>                 Password for user [none]\n" +
                "  -ssl-truststore-path <path>    Path to SSL truststore [none]\n" +
                "  -ssl-truststore-pwd <pwd>       Password for SSL truststore [none]\n" +
                "  -ssl-keystore-path <path>      Path to SSL keystore [none]\n" +
                "  -ssl-keystore-pwd <pwd>         Password for SSL keystore [none]\n" +
                "  -consistencyLevel <CL>         Consistency level [LOCAL_ONE]\n" +
                "  -decimalDelim <decimalDelim>   Decimal delimiter [.] Other option is ','\n" +
                "  -boolStyle <boolStyleString>   Style for booleans [TRUE_FALSE]\n" +
                "  -maxCharsPerColumn <int>       Buffer size for parsing columns [4096]\n" +
                "  -numThreads <numThreads>       Number of concurrent threads to unload [5]\n";
    }

    protected boolean validateArgs() {
        if (numThreads < 1) {
            System.err.println("Number of threads must be non-negative");
            return false;
        }
        if ((null == username) && (null != password)) {
            System.err.println("If you supply the password, you must supply the username");
            return false;
        }
        if ((null != username) && (null == password)) {
            System.err.println("If you supply the username, you must supply the password");
            return false;
        }
        if ((null == this.truststorePath) && (null != truststorePwd)) {
            System.err.println("If you supply the ssl-truststore-pwd, you must supply the ssl-truststore-path");
            return false;
        }
        if ((null != this.truststorePath) && (null == truststorePwd)) {
            System.err.println("If you supply the ssl-truststore-path, you must supply the ssl-truststore-pwd");
            return false;
        }
        final String keystorePath = this.keystorePath;
        if ((null == keystorePath) && (null != keystorePwd)) {
            System.err.println("If you supply the ssl-keystore-pwd, you must supply the ssl-keystore-path");
            return false;
        }
        if ((null != keystorePath) && (null == keystorePwd)) {
            System.err.println("If you supply the ssl-keystore-path, you must supply the ssl-keystore-pwd");
            return false;
        }
        if (checkFile(this.truststorePath, "truststore file must be a file")) return false;
        if (checkFile(keystorePath, "keystore file must be a file")) return false;

        return true;
    }

}
