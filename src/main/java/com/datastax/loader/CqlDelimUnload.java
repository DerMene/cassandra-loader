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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.loader.parser.BooleanParser;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;


public class CqlDelimUnload extends ConfigurationLoader {
    private String beginToken = "-9223372036854775808";
    private String endToken = "9223372036854775807";
    private String where = null;

    public static void main(String[] args)
            throws IOException, ParseException, InterruptedException, ExecutionException,
            KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException,
            CertificateException, KeyManagementException {
        CqlDelimUnload cdu = new CqlDelimUnload();
        boolean success = cdu.run(args);
        if (success) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }

    protected String usage() {
        StringBuilder usage = new StringBuilder("version: ").append(version).append("\n");
        usage.append("Usage: -f <outputStem> -host <ipaddress> -schema <schema> [OPTIONS]\n");
        usage.append("OPTIONS:\n");
        usage.append(commonUsage());

        usage.append("  -beginToken <tokenString>      Begin token [none]\n");
        usage.append("  -endToken <tokenString>        End token [none]\n");
        usage.append("  -where <predicate>             WHERE clause [none]\n");
        return usage.toString();
    }

    @Override
    protected boolean validateArgs() {
        super.validateArgs();
        if ((null != beginToken) && (null == endToken)) {
            System.err.println("If you supply the beginToken then you need to specify the endToken");
            return false;
        }
        if ((null == beginToken) && (null != endToken)) {
            System.err.println("If you supply the endToken then you need to specify the beginToken");
            return false;
        }

        return true;
    }


    protected boolean parseArgsFromMap(Map<String, String> amap) {
        super.parseArgsFromMap(amap);

        String tkey;
        if (null != (tkey = amap.remove("-beginToken"))) beginToken = tkey;
        if (null != (tkey = amap.remove("-endToken"))) endToken = tkey;
        if (null != (tkey = amap.remove("-where"))) where = tkey;

        return true;
    }

    @Override
    protected int getNumConnections() {
        return 4;
    }

    public boolean run(String[] args)
            throws IOException, InterruptedException, ExecutionException,
            KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
            CertificateException, UnrecoverableKeyException {
        if (!parseArgs(args)) {
            System.err.println("Bad arguments");
            System.err.println(usage());
            return false;
        }

        // Setup
        if (!setup())
            return false;

        PrintStream pstream = null;
        if (1 == numThreads) {
            if (filename.equalsIgnoreCase("stdout")) {
                pstream = System.out;
            } else {
                pstream = new PrintStream(new BufferedOutputStream(new FileOutputStream(filename + ".0")));
            }
            beginToken = null;
            endToken = null;
        }

        // Launch Threads
        ExecutorService executor;
        long total = 0;
        if (null != pstream) {
            // One file/stdin to process
            executor = Executors.newSingleThreadExecutor();
            Callable<Long> worker = new ThreadExecute(cqlSchema, delimiter,
                    nullString,
                    dateFormatString,
                    boolStyle, locale,
                    pstream,
                    beginToken,
                    endToken, session,
                    consistencyLevel, where);
            Future<Long> res = executor.submit(worker);
            total = res.get();
            executor.shutdown();
        } else {
            BigInteger begin;
            BigInteger end;
            BigInteger delta;
            List<String> beginList = new ArrayList<>();
            List<String> endList = new ArrayList<>();
            if (null != beginToken) {
                begin = new BigInteger(beginToken);
                end = new BigInteger(endToken);
                delta = end.subtract(begin).divide(new BigInteger(String.valueOf(numThreads)));
                for (int mype = 0; mype < numThreads; mype++) {
                    if (mype < numThreads - 1) {
                        beginList.add(begin.add(delta.multiply(new BigInteger(String.valueOf(mype)))).toString());
                        endList.add(begin.add(delta.multiply(new BigInteger(String.valueOf(mype + 1)))).toString());
                    } else {
                        beginList.add(begin.add(delta.multiply(new BigInteger(String.valueOf(numThreads - 1)))).toString());
                        endList.add(end.toString());
                    }
                }
            } else {
                // What's the right thing here?
                // (1) Split into canonical token ranges - numThreads=numRanges
                // (2) Split into subranges of canonical token ranges
                //     - if numThreads < numRanges, then reset numThreads=numRanges
                //     - let K=CEIL(numThreads/numRanges) and M=MOD(numThreads/numRanges), for the first M token ranges split into K subranges, and for the remaining ones split into K-1 subranges
                // (?) Should there be an option for numThreads-per-range?
                // (?) Should there be an option for numThreads=numRanges
            }

            executor = Executors.newFixedThreadPool(numThreads);
            Set<Future<Long>> results = new HashSet<>();
            for (int mype = 0; mype < numThreads; mype++) {
                String tBeginString = beginList.get(mype);
                String tEndString = endList.get(mype);
                pstream = new PrintStream(new BufferedOutputStream(new FileOutputStream(filename + "." + mype)));
                Callable<Long> worker = new ThreadExecute(cqlSchema, delimiter,
                        nullString,
                        dateFormatString,
                        boolStyle, locale,
                        pstream,
                        tBeginString,
                        tEndString, session,
                        consistencyLevel,
                        where);
                results.add(executor.submit(worker));
            }
            executor.shutdown();
            for (Future<Long> res : results)
                total += res.get();
        }
        System.err.println("Total rows retrieved: " + total);

        // Cleanup
        cleanup();

        return true;
    }

    class ThreadExecute implements Callable<Long> {
        private final Session session;
        private final ConsistencyLevel consistencyLevel;
        private final String cqlSchema;
        private PreparedStatement statement;
        private CqlDelimParser cdp;
        private Locale locale = null;
        private BooleanParser.BoolStyle boolStyle = null;
        private String nullString = null;
        private String delimiter = null;

        private PrintStream writer = null;
        private String beginToken = null;
        private String endToken = null;
        private long numRead = 0;
        private String where = null;

        public ThreadExecute(String inCqlSchema, String inDelimiter,
                             String inNullString,
                             String inDateFormatString,
                             BooleanParser.BoolStyle inBoolStyle,
                             Locale inLocale,
                             PrintStream inWriter,
                             String inBeginToken, String inEndToken,
                             Session inSession, ConsistencyLevel inConsistencyLevel,
                             String inWhere) {
            super();
            cqlSchema = inCqlSchema;
            delimiter = inDelimiter;
            nullString = inNullString;
            dateFormatString = inDateFormatString;
            boolStyle = inBoolStyle;
            locale = inLocale;
            beginToken = inBeginToken;
            endToken = inEndToken;
            session = inSession;
            writer = inWriter;
            consistencyLevel = inConsistencyLevel;
            where = inWhere;
        }

        public Long call() throws IOException, ParseException {
            if (!setup()) {
                return 0L;
            }
            numRead = execute();
            cleanup();
            return numRead;
        }

        private String getPartitionKey(CqlDelimParser cdp, Session session) {
            String keyspace = cdp.getKeyspace();
            String table = cdp.getTable();
            if (keyspace.startsWith("\"") && keyspace.endsWith("\""))
                keyspace = keyspace.replaceAll("\"", "");
            else
                keyspace = keyspace.toLowerCase();
            if (table.startsWith("\"") && table.endsWith("\""))
                table = table.replaceAll("\"", "");
            else
                table = table.toLowerCase();

            List<ColumnMetadata> lcm = session.getCluster().getMetadata()
                    .getKeyspace(keyspace).getTable(table).getPartitionKey();
            String partitionKey = lcm.get(0).getName();
            for (int i = 1; i < lcm.size(); i++) {
                partitionKey = partitionKey + "," + lcm.get(i).getName();
            }
            return partitionKey;
        }

        private boolean setup() throws ParseException {
            cdp = new CqlDelimParser(cqlSchema, delimiter, nullString,
                    dateFormatString,
                    boolStyle, locale, null, session, false);
            String select = cdp.generateSelect();
            String partitionKey = getPartitionKey(cdp, session);
            if (null != beginToken) {
                select = select + " WHERE Token(" + partitionKey + ") > "
                        + beginToken + " AND Token(" + partitionKey + ") <= "
                        + endToken;
                if (null != where)
                    select = select + " AND " + where;
            } else {
                if (null != where)
                    select = select + " WHERE " + where;
            }
            try {
                statement = session.prepare(select);
            } catch (QueryValidationException iqe) {
                System.err.println("Error creating statement: " + iqe.getMessage());
                System.err.println("CQL Query: " + select);
                if (null != where)
                    System.err.println("Check your syntax for -where: " + where);
                return false;
            }
            statement.setConsistencyLevel(consistencyLevel);
            return true;
        }

        private void cleanup() {
            writer.flush();
            writer.close();
        }

        private long execute() {
            BoundStatement bound = statement.bind();
            ResultSet rs = session.execute(bound);
            numRead = 0;
            for (Row row : rs) {
                writer.println(cdp.format(row));
                numRead++;
            }
            return numRead;
        }
    }
}

