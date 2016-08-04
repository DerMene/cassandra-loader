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
import com.datastax.loader.futures.FutureManager;
import com.datastax.loader.futures.PrintingFutureSet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.Callable;

class CqlDelimLoadTask implements Callable<Long> {
    private final CqlDelimParser cdp;
    private static final String BADPARSE = ".BADPARSE";
    private static final String BADINSERT = ".BADINSERT";
    private static final String LOG = ".LOG";
    private final Session session;
    private PreparedStatement statement;
    private final ConsistencyLevel consistencyLevel;
    private final long maxErrors;
    private long skipRows;
    private long maxRows;
    private final String badDir;
    private final String successDir;
    private final String failureDir;
    private String readerName;
    private PrintStream badParsePrinter = null;
    private PrintStream badInsertPrinter = null;
    private PrintStream logPrinter = null;
    private String logFname = "";
    private BufferedReader reader;
    private final File infile;
    private final int numFutures;
    private final int batchSize;

    private long queryTimeout = 2;
    private int numRetries = 1;
    private long maxInsertErrors = 10;
    private final boolean nullsUnset;

    public CqlDelimLoadTask(long inMaxErrors, long inSkipRows,
                            long inMaxRows,
                            String inBadDir, File inFile,
                            Session inSession, ConsistencyLevel inCl,
                            int inNumFutures, int inBatchSize, int inNumRetries,
                            int inQueryTimeout, long inMaxInsertErrors,
                            String inSuccessDir, String inFailureDir,
                            boolean inNullsUnset, CqlDelimParser inCdp) {
        maxErrors = inMaxErrors;
        skipRows = inSkipRows;
        maxRows = inMaxRows;
        badDir = inBadDir;
        infile = inFile;
        session = inSession;
        consistencyLevel = inCl;
        numFutures = inNumFutures;
        batchSize = inBatchSize;
        numRetries = inNumRetries;
        queryTimeout = inQueryTimeout;
        maxInsertErrors = inMaxInsertErrors;
        successDir = inSuccessDir;
        failureDir = inFailureDir;
        nullsUnset = inNullsUnset;
        cdp = inCdp;
    }

    public Long call() throws IOException, ParseException {
        setup();
        long numInserted = execute();
        return numInserted;
    }

    private void setup() throws IOException {
        if (null == infile) {
            reader = new BufferedReader(new InputStreamReader(System.in));
            readerName = "stdin";
        } else {
            reader = new BufferedReader(new FileReader(infile));
            readerName = infile.getName();
        }

        // Prepare Badfile
        if (null != badDir) {
            badParsePrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(badDir + "/" + readerName + BADPARSE)));
            badInsertPrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(badDir + "/" + readerName + BADINSERT)));
            logFname = badDir + "/" + readerName + LOG;
            logPrinter = new PrintStream(new BufferedOutputStream(new FileOutputStream(logFname)));
        }


        String insert = cdp.generateInsert();
        statement = session.prepare(insert);
        statement.setRetryPolicy(new LoaderRetryPolicy(numRetries));
        statement.setConsistencyLevel(consistencyLevel);
    }

    private void cleanup(boolean success) throws IOException {
        if (null != badParsePrinter)
            badParsePrinter.close();
        if (null != badInsertPrinter)
            badInsertPrinter.close();
        if (null != logPrinter)
            logPrinter.close();
        if (success) {
            if (null != successDir) {
                Path src = infile.toPath();
                Path dst = Paths.get(successDir);
                Files.move(src, dst.resolve(src.getFileName()),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        } else {
            if (null != failureDir) {
                Path src = infile.toPath();
                Path dst = Paths.get(failureDir);
                Files.move(src, dst.resolve(src.getFileName()),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private long execute() throws IOException {
        FutureManager fm = new PrintingFutureSet(numFutures, queryTimeout,
                maxInsertErrors,
                logPrinter,
                badInsertPrinter);
        String line;
        int lineNumber = 0;
        long numInserted = 0;
        int numErrors = 0;
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        ResultSetFuture resultSetFuture;
        BoundStatement bind;
        List<Object> elements;

        System.err.println("*** Processing " + readerName);
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            if (skipRows > 0) {
                skipRows--;
                continue;
            }
            if (maxRows-- < 0)
                break;

            if (0 == line.trim().length())
                continue;

            if (null != (elements = cdp.parse(line))) {
                bind = statement.bind(elements.toArray());
                if (nullsUnset) {
                    for (int i = 0; i < elements.size(); i++)
                        if (null == elements.get(i))
                            bind.unset(i);
                }
                if (1 == batchSize) {
                    resultSetFuture = session.executeAsync(bind);
                    if (!fm.add(resultSetFuture, line)) {
                        System.err.println("There was an error.  Please check the log file for more information (" + logFname + ")");
                        cleanup(false);
                        return -2;
                    }
                    numInserted += 1;
                } else {
                    batch.add(bind);
                    if (batchSize == batch.size()) {
                        resultSetFuture = session.executeAsync(batch);
                        if (!fm.add(resultSetFuture, line)) {
                            System.err.println("There was an error.  Please check the log file for more information (" + logFname + ")");
                            cleanup(false);
                            return -2;
                        }
                        numInserted += batch.size();
                        batch.clear();
                    }
                }
            } else {
                if (null != logPrinter) {
                    logPrinter.println(String.format("Error parsing line %d in %s: %s", lineNumber, readerName, line));
                }
                System.err.println(String.format("Error parsing line %d in %s: %s", lineNumber, readerName, line));
                if (null != badParsePrinter) {
                    badParsePrinter.println(line);
                }
                numErrors++;
                if (maxErrors <= numErrors) {
                    if (null != logPrinter) {
                        logPrinter.println(String.format("Maximum number of errors exceeded (%d) for %s", numErrors, readerName));
                    }
                    System.err.println(String.format("Maximum number of errors exceeded (%d) for %s", numErrors, readerName));
                    cleanup(false);
                    return -1;
                }
            }
        }
        if ((batchSize > 1) && (batch.size() > 0)) {
            resultSetFuture = session.executeAsync(batch);
            if (!fm.add(resultSetFuture, line)) {
                cleanup(false);
                return -2;
            }
            numInserted += batch.size();
        }

        if (!fm.cleanup()) {
            cleanup(false);
            return -1;
        }

        if (null != logPrinter) {
            logPrinter.println("*** DONE: " + readerName + "  number of lines processed: " + lineNumber + " (" + numInserted + " inserted)");
        }
        System.err.println("*** DONE: " + readerName + "  number of lines processed: " + lineNumber + " (" + numInserted + " inserted)");

        cleanup(true);
        return fm.getNumInserted();
    }
}
