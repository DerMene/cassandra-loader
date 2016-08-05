package com.datastax.loader.futures;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

public class PrintingFutureAction implements FutureAction {
    protected final long period = 100000;
    protected final AtomicLong numInserted;
    protected PrintStream logPrinter = null;
    protected PrintStream badInsertPrinter = null;

    public PrintingFutureAction(PrintStream inLogPrinter,
                                PrintStream inBadInsertPrinter) {
        logPrinter = inLogPrinter;
        badInsertPrinter = inBadInsertPrinter;
        numInserted = new AtomicLong(0);
    }

    public void onSuccess() {
        if (logPrinter != null) {
            long cur = numInserted.incrementAndGet();
            if (0 == (cur % period)) {
                logPrinter.println("Progress:  " + cur);
            }
        }
    }

    public void onFailure(Throwable t, String line) {
        if (logPrinter != null) {
            logPrinter.println("Error inserting: " + t.getMessage());
            t.printStackTrace(logPrinter);
        }
        if (badInsertPrinter != null) {
            badInsertPrinter.println(line);
        }
    }

    public void onTooManyFailures() {
        if (logPrinter != null) {
            logPrinter.println("Too many INSERT errors ... Stopping");
        }
    }

}
