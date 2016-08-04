package com.datastax.loader.futures;

import com.datastax.driver.core.ResultSetFuture;

import java.util.concurrent.TimeUnit;

public abstract class AbstractFutureManager implements FutureManager {
    protected final int size;
    protected final long queryTimeout;
    protected final long maxInsertErrors;
    protected final TimeUnit unit = TimeUnit.SECONDS;

    public AbstractFutureManager(int inSize, long inQueryTimeout, long inMaxInsertErrors) {
        size = inSize;
        queryTimeout = inQueryTimeout;
        maxInsertErrors = inMaxInsertErrors;
    }

    public abstract boolean add(ResultSetFuture future, String line);

    public abstract boolean cleanup();

    public abstract long getNumInserted();
}
