/*
 * Copyright 2015 Brian Hess
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.loader;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;

public class CqlDelimLoad extends ConfigurationLoader {
    public static final String STDIN = "stdin";
    public static final String STDERR = "stderr";
    private int numFutures = 1000;
    private int inNumFutures = -1;
    private int queryTimeout = 2;
    private long maxInsertErrors = 10;
    private int numRetries = 1;
    private double rate = 50000.0;
    private long progressRate = 100000;
    private RateLimiter rateLimiter = null;
    private String rateFile = null;
    private PrintStream rateStream = null;
    private long maxErrors = 10;
    private long skipRows = 0;
    private String skipCols = null;
    private long maxRows = -1;
    private String badDir = ".";
    private String successDir = null;
    private String failureDir = null;

    private String filePattern = null;

    private int numThreads = Runtime.getRuntime().availableProcessors();
    private int batchSize = 1;
    private boolean nullsUnset = false;

    public static void main(String[] args)
            throws IOException, ParseException, InterruptedException, ExecutionException,
            KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException,
            CertificateException, KeyManagementException {
        CqlDelimLoad cdl = new CqlDelimLoad();
        boolean success = cdl.run(args);
        if (success) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }


    private String usage() {
        StringBuilder usage = new StringBuilder("version: ").append(version).append("\n");
        usage.append("Usage: -f <filename|directory> -host <ipaddress> -schema <schema> [OPTIONS]\n");
        usage.append("OPTIONS:\n");
        usage.append(commonUsage());

        usage.append("  -skipRows <skipRows>           Number of rows to skip [0]\n");
        usage.append("  -skipCols <columnsToSkip>      Comma-separated list of columsn to skip in the input file\n");
        usage.append("  -maxRows <maxRows>             Maximum number of rows to read (-1 means all) [-1]\n");
        usage.append("  -maxErrors <maxErrors>         Maximum parse errors to endure [10]\n");
        usage.append("  -badDir <badDirectory>         Directory for where to place badly parsed rows. [none]\n");
        usage.append("  -numFutures <numFutures>       Number of CQL futures to keep in flight [1000]\n");
        usage.append("  -batchSize <batchSize>         Number of INSERTs to batch together [1]\n");
        usage.append("  -queryTimeout <# seconds>      Query timeout (in seconds) [2]\n");
        usage.append("  -numRetries <numRetries>       Number of times to retry the INSERT [1]\n");
        usage.append("  -maxInsertErrors <# errors>    Maximum INSERT errors to endure [10]\n");
        usage.append("  -rate <rows-per-second>        Maximum insert rate [50000]\n");
        usage.append("  -progressRate <num txns>       How often to report the insert rate [100000]\n");
        usage.append("  -rateFile <filename>           Where to print the rate statistics\n");
        usage.append("  -successDir <dir>              Directory where to move successfully loaded files\n");
        usage.append("  -failureDir <dir>              Directory where to move files that did not successfully load\n");
        usage.append("  -nullsUnset [false|true]       Treat nulls as unset [false]\n");
        usage.append("  -filePattern <pattern>         When -f is a folder: use only files matching this pattern [all files]\n");

        usage.append("\n\nExamples:\n");
        usage.append("cassandra-loader -f /path/to/file.csv -host localhost -schema \"test.test3(a, b, c)\"\n");
        usage.append("cassandra-loader -f /path/to/directory -host 1.2.3.4 -schema \"test.test3(a, b, c)\" -delim \"\\t\" -numThreads 10\n");
        usage.append("cassandra-loader -f stdin -host localhost -schema \"test.test3(a, b, c)\" -user myuser -pw mypassword\n");
        return usage.toString();
    }

    @Override
    protected boolean validateArgs() {
        super.validateArgs();
        if (0 >= numFutures) {
            System.err.println("Number of futures must be positive (" + numFutures + ")");
            return false;
        }
        if (0 >= batchSize) {
            System.err.println("Batch size must be positive (" + batchSize + ")");
            return false;
        }
        if (0 >= queryTimeout) {
            System.err.println("Query timeout must be positive");
            return false;
        }
        if (0 > maxInsertErrors) {
            System.err.println("Maximum number of insert errors must be non-negative");
            return false;
        }
        if (0 > numRetries) {
            System.err.println("Number of retries must be non-negative");
            return false;
        }
        if (0 > skipRows) {
            System.err.println("Number of rows to skip must be non-negative");
            return false;
        }
        if (0 >= maxRows) {
            System.err.println("Maximum number of rows to load must be positive");
            return false;
        }
        if (0 > maxErrors) {
            System.err.println("Maximum number of parse errors must be non-negative");
            return false;
        }
        if (0 > progressRate) {
            System.err.println("Progress rate must be non-negative");
            return false;
        }
        if (!STDIN.equalsIgnoreCase(filename)) {
            File infile = new File(filename);
            if ((!infile.isFile()) && (!infile.isDirectory())) {
                System.err.println("The -f argument needs to be a file or a directory");
                return false;
            }
            if (infile.isDirectory()) {
                File[] infileList = infile.listFiles();
                if (infileList.length < 1) {
                    System.err.println("The directory supplied is empty");
                    return false;
                }
            }
        }
        if (null != successDir) {
            if (STDIN.equalsIgnoreCase(filename)) {
                System.err.println("Cannot specify -successDir with stdin");
                return false;
            }
            File sdir = new File(successDir);
            if (!sdir.isDirectory()) {
                System.err.println("-successDir must be a directory");
                return false;
            }
        }
        if (null != failureDir) {
            if (STDIN.equalsIgnoreCase(filename)) {
                System.err.println("Cannot specify -failureDir with stdin");
                return false;
            }
            File sdir = new File(failureDir);
            if (!sdir.isDirectory()) {
                System.err.println("-failureDir must be a directory");
                return false;
            }
        }

        if (0 > rate) {
            System.err.println("Rate must be positive");
            return false;
        }

        return true;
    }

    protected boolean parseArgsFromMap(Map<String, String> amap) {
        super.parseArgsFromMap(amap);

        String tkey;
        if (null != (tkey = amap.remove("-numFutures"))) inNumFutures = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-batchSize"))) batchSize = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-queryTimeout"))) queryTimeout = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-maxInsertErrors"))) maxInsertErrors = Long.parseLong(tkey);
        if (null != (tkey = amap.remove("-numRetries"))) numRetries = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-maxErrors"))) maxErrors = Long.parseLong(tkey);
        if (null != (tkey = amap.remove("-skipRows"))) skipRows = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-skipCols"))) skipCols = tkey;
        if (null != (tkey = amap.remove("-maxRows"))) maxRows = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-badDir"))) badDir = tkey;
        if (null != (tkey = amap.remove("-filePattern"))) {
            try {
                FileSystems.getDefault().getPathMatcher(tkey);
            } catch (Throwable ignored) {
                System.err.println("Bad filePattern parameter. See https://docs.oracle.com/javase/7/docs/api/java/nio/file/FileSystem.html#getPathMatcher(java.lang.String) for usage");
                return false;
            }
            filePattern = tkey;
        }
        if (null != (tkey = amap.remove("-numThreads"))) numThreads = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-rate"))) rate = Double.parseDouble(tkey);
        if (null != (tkey = amap.remove("-progressRate"))) progressRate = Long.parseLong(tkey);
        if (null != (tkey = amap.remove("-rateFile"))) rateFile = tkey;
        if (null != (tkey = amap.remove("-successDir"))) successDir = tkey;
        if (null != (tkey = amap.remove("-failureDir"))) failureDir = tkey;
        if (null != (tkey = amap.remove("-nullsUnset"))) nullsUnset = Boolean.parseBoolean(tkey);

        if (-1 == maxRows) {
            maxRows = Long.MAX_VALUE;
        }
        if (-1 == maxErrors) {
            maxErrors = Long.MAX_VALUE;
        }
        if (-1 == maxInsertErrors) {
            maxInsertErrors = Long.MAX_VALUE;
        }

        if (!amap.isEmpty()) {
            for (String k : amap.keySet()) {
                System.err.println("Unrecognized option: " + k);
            }
            return false;
        }

        if (0 < inNumFutures) {
            numFutures = inNumFutures / numThreads;
        }

        return true;
    }

    @Override
    protected int getNumConnections() {
        return 8;
    }

    @Override
    protected Session getSession(Cluster cluster) throws FileNotFoundException {
        Session tsession = cluster.connect();

        if ((0 > cluster.getConfiguration().getProtocolOptions()
                .getProtocolVersion().compareTo(ProtocolVersion.V4))
                && nullsUnset) {
            System.err.println("Cannot use nullsUnset with ProtocolVersion less than V4 (prior to Cassandra 3.0");
            cleanup();
            return null;
        }

        if (null != rateFile) {
            if (STDERR.equalsIgnoreCase(rateFile)) {
                rateStream = System.err;
            } else {
                rateStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(rateFile)), true);
            }
        }
        Metrics metrics = cluster.getMetrics();
        com.codahale.metrics.Timer timer = metrics.getRequestsTimer();
        rateLimiter = new RateLimiter(rate, progressRate, timer, rateStream);
        //rateLimiter = new Latency999RateLimiter(rate, progressRate, 3000, 200, 10, 0.5, 0.1, cluster, false);
        session = new RateLimitedSession(tsession, rateLimiter);
        return session;
    }

    @Override
    protected void cleanup() {
        super.cleanup();
        rateLimiter.report(null, null);
        if (null != rateStream)
            rateStream.close();
    }

    public boolean run(String[] args)
            throws IOException, ParseException, InterruptedException, ExecutionException, KeyStoreException,
            NoSuchAlgorithmException, KeyManagementException, CertificateException,
            UnrecoverableKeyException {
        if (!parseArgs(args)) {
            System.err.println("Bad arguments");
            System.err.println(usage());
            return false;
        }

        // Setup
        if (!setup())
            return false;

        // open file
        Deque<File> fileList = new ArrayDeque<>();
        File infile;
        File[] inFileList;
        boolean onefile = true;
        if (STDIN.equalsIgnoreCase(filename)) {
            infile = null;
        } else {
            infile = new File(filename);
            if (!infile.isFile()) {
                inFileList = infile.listFiles();
                if (inFileList.length < 1)
                    throw new IOException("directory is empty");
                onefile = false;
                Arrays.sort(inFileList,
                        (f1, f2) -> f1.getName().compareTo(f2.getName()));
                PathMatcher matcher = null;
                if (filePattern != null) {
                    matcher = FileSystems.getDefault().getPathMatcher(filePattern);
                }
                for (final File file : inFileList) {
                    if (matcher == null || matcher.matches(file.toPath().getFileName())) {
                        fileList.push(file);
                    }
                }
            }
        }

        final CqlDelimParser cqlDelimParser = new CqlDelimParser(cqlSchema, delimiter, nullString,
                dateFormatString, boolStyle, locale,
                skipCols, session, true, quote, escape, maxCharsPerColumn);

        // Launch Threads
        ExecutorService executor;
        long total = 0;
        if (onefile) {
            // One file/stdin to process
            executor = Executors.newSingleThreadExecutor();
            Callable<Long> worker = getWorker(infile, cqlDelimParser);
            Future<Long> res = executor.submit(worker);
            total = res.get();
            executor.shutdown();
        } else {
            executor = Executors.newFixedThreadPool(numThreads);
            Set<Future<Long>> results = new HashSet<>();
            while (!fileList.isEmpty()) {
                File tFile = fileList.pop();
                Callable<Long> worker = getWorker(tFile, cqlDelimParser);
                results.add(executor.submit(worker));
            }
            executor.shutdown();
            for (Future<Long> res : results)
                total += res.get();
        }

        // Cleanup
        cleanup();
        //System.err.println("Total rows inserted: " + total);

        return true;
    }

    private Callable<Long> getWorker(File tFile, CqlDelimParser cqlDelimParser) {
        return new CqlDelimLoadTask(
                maxErrors, skipRows,
                maxRows, badDir, tFile,
                session,
                consistencyLevel,
                numFutures, batchSize,
                numRetries,
                queryTimeout,
                maxInsertErrors,
                successDir, failureDir,
                nullsUnset, cqlDelimParser);
    }
}

