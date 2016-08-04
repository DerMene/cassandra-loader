package com.datastax.loader.futures;

import java.io.PrintStream;

public class PrintingFutureList extends ActionFutureList {

    public PrintingFutureList(int inSize, long inQueryTimeout, long inMaxInsertErrors,
                              PrintStream inLogPrinter, PrintStream inBadInsertPrinter) {
        super(inSize, inQueryTimeout, inMaxInsertErrors, new PrintingFutureAction(inLogPrinter, inBadInsertPrinter));
    }
}
