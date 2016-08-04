package com.datastax.loader;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.loader.parser.BooleanParser;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static com.datastax.loader.CqlDelimLoad.STDERR;
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
        if (filename.equalsIgnoreCase("stdout")) {
            numThreads = 1;
        }

        return true;
    }

    protected boolean parseArgs(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("No arguments specified");
            return false;
        }
        if (0 != args.length % 2) {
            System.err.println("Not an even number of parameters");
            return false;
        }
        Map<String, String> amap = new HashMap<>();
        for (int i = 0; i < args.length; i += 2)
            amap.put(args[i], args[i + 1]);

        String tkey;
        if (null != (tkey = amap.remove("-configFile")))
            if (!processConfigFile(tkey, amap))
                return false;

        host = amap.remove("-host");
        if (null == host) { // host is required
            System.err.println("Must provide a host");
            return false;
        }

        return validateArgs();
    }

    protected boolean parseArgsFromMap(Map<String, String> amap) {
        filename = amap.remove("-f");
        if (null == filename) { // filename is required
            System.err.println("Must provide a filename/directory");
            return false;
        }
        cqlSchema = amap.remove("-schema");
        if (null == cqlSchema) { // schema is required
            System.err.println("Must provide a schema");
            return false;
        }

        String tkey;
        if (null != (tkey = amap.remove("-port"))) port = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-user"))) username = tkey;
        if (null != (tkey = amap.remove("-pw"))) password = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-path"))) truststorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-pwd"))) truststorePwd = tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-path"))) keystorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-pwd"))) keystorePwd = tkey;
        if (null != (tkey = amap.remove("-consistencyLevel"))) consistencyLevel = ConsistencyLevel.valueOf(tkey);
        if (null != (tkey = amap.remove("-dateFormat"))) dateFormatString = tkey;
        if (null != (tkey = amap.remove("-nullString"))) nullString = tkey;
        if (null != (tkey = amap.remove("-delim"))) delimiter = tkey;
        if (null != (tkey = amap.remove("-maxCharsPerColumn"))) maxCharsPerColumn = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-quote"))) {
            if (tkey.length() != 1) {
                System.err.println("Bad quote parameter, must be single character.");
            }
            quote = tkey.charAt(0);
        }
        if (null != (tkey = amap.remove("-escape"))) {
            if (tkey.length() != 1) {
                System.err.println("Bad escape parameter, must be single character.");
            }
            escape = tkey.charAt(0);
        }
        if (null != (tkey = amap.remove("-decimalDelim"))) {
            if (tkey.equals(","))
                locale = Locale.FRANCE;
        }
        if (null != (tkey = amap.remove("-boolStyle"))) {
            boolStyle = BooleanParser.getBoolStyle(tkey);
            if (null == boolStyle) {
                System.err.println("Bad boolean style.  Options are: " + BooleanParser.getOptions());
                return false;
            }
        }

        return true;
    }

    private SSLOptions createSSLOptions()
            throws KeyStoreException, IOException, NoSuchAlgorithmException,
            KeyManagementException, CertificateException, UnrecoverableKeyException {
        TrustManagerFactory tmf;
        KeyStore tks = KeyStore.getInstance("JKS");
        tks.load(new FileInputStream(new File(truststorePath)),
                truststorePwd.toCharArray());
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(tks);

        KeyManagerFactory kmf = null;
        if (null != keystorePath) {
            KeyStore kks = KeyStore.getInstance("JKS");
            kks.load(new FileInputStream(new File(keystorePath)),
                    keystorePwd.toCharArray());
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(kks, keystorePwd.toCharArray());
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf != null ? kmf.getKeyManagers() : null,
                tmf.getTrustManagers(),
                new SecureRandom());

        return JdkSSLOptions.builder().withSSLContext(sslContext).build();
    }

    private Cluster getCluster(int numConnections)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
            CertificateException, UnrecoverableKeyException {
        // Connect to Cassandra
        PoolingOptions pOpts = new PoolingOptions();
        pOpts.setMaxConnectionsPerHost(HostDistance.LOCAL, numConnections);
        pOpts.setCoreConnectionsPerHost(HostDistance.LOCAL, numConnections);
        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoint(host)
                .withPort(port)
                //.withCompression(ProtocolOptions.Compression.LZ4)
                .withPoolingOptions(pOpts)
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()));

        if (null != username)
            clusterBuilder = clusterBuilder.withCredentials(username, password);
        if (null != truststorePath)
            clusterBuilder = clusterBuilder.withSSL(createSSLOptions());

        final Cluster cluster = clusterBuilder.build();
        if (null == cluster) {
            throw new IOException("Could not create cluster");
        }

        return cluster;
    }

    public boolean setup()
            throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
            CertificateException, UnrecoverableKeyException {
        cluster = getCluster(getNumConnections());
        session = getSession(cluster);

        if (session == null) {
            return false;
        }

        return true;
    }

    protected int getNumConnections() {
        return 8;
    }

    protected Session getSession(Cluster cluster) throws FileNotFoundException {
        return cluster.connect();
    }


    protected void cleanup() {
        if (null != session)
            session.close();
        if (null != cluster)
            cluster.close();
    }
}