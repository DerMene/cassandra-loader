package com.datastax.loader.futures;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class ActionFutureSet extends AbstractFutureManager {
    protected final Semaphore available;
    protected final AtomicLong insertErrors;
    protected final AtomicLong numInserted;
    protected FutureAction futureAction = null;

    public ActionFutureSet(int inSize, long inQueryTimeout,
                           long inMaxInsertErrors,
                           FutureAction inFutureAction) {
        super(inSize, inQueryTimeout, inMaxInsertErrors);
        futureAction = inFutureAction;
        available = new Semaphore(size, true);
        insertErrors = new AtomicLong(0);
        numInserted = new AtomicLong(0);
    }

    public boolean add(ResultSetFuture future, final String line) {
        if (maxInsertErrors <= insertErrors.get())
            return false;
        try {
            available.acquire();
        } catch (InterruptedException e) {
            return false;
        }
        Futures.addCallback(future, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet rs) {
                available.release();
                numInserted.incrementAndGet();
                futureAction.onSuccess();
            }

            @Override
            public void onFailure(Throwable t) {
                available.release();
                long numErrors = insertErrors.incrementAndGet();
                futureAction.onFailure(t, line);
                if (maxInsertErrors <= numErrors) {
                    futureAction.onTooManyFailures();
                }
            }
        });
        return true;
    }

    public boolean cleanup() {
        try {
            available.acquire(this.size);
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    public long getNumInserted() {
        return numInserted.get();
    }
}
