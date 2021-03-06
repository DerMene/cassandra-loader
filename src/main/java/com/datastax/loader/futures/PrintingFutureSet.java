package com.datastax.loader.futures;

import java.io.PrintStream;

public class PrintingFutureSet extends ActionFutureSet {

    public PrintingFutureSet(int inSize, long inQueryTimeout,
                             long inMaxInsertErrors,
                             PrintStream inLogPrinter,
                             PrintStream inBadInsertPrinter) {
        super(inSize, inQueryTimeout, inMaxInsertErrors,
                new PrintingFutureAction(inLogPrinter, inBadInsertPrinter));
    }
}
