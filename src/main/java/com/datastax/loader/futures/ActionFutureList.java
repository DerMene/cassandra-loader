package com.datastax.loader.futures;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

import java.util.ArrayList;
import java.util.List;

public class ActionFutureList extends AbstractFutureManager {
    protected final List<ResultSetFuture> futures;
    protected final List<String> strings;
    protected long insertErrors;
    protected long numInserted;
    protected FutureAction futureAction = null;

    public ActionFutureList(int inSize, long inQueryTimeout, long inMaxInsertErrors, FutureAction inFutureAction) {
        super(inSize, inQueryTimeout, inMaxInsertErrors);
        futureAction = inFutureAction;
        futures = new ArrayList<>(size);
        strings = new ArrayList<>(size);
        insertErrors = 0;
        numInserted = 0;
    }

    public boolean add(ResultSetFuture future, String line) {
        if (futures.size() >= size) {
            if (!purgeFutures())
                return false;
        }
        futures.add(future);
        strings.add(line);
        numInserted++;
        return true;
    }

    protected boolean purgeFutures() {
        if (0 == futures.size())
            return true;
        for (int i = 0; i < futures.size(); i++) {
            ResultSetFuture future = futures.get(i);
            String line = strings.get(i);
            try {
                //long beginTime = System.currentTimeMillis();
                ResultSet rs = future.getUninterruptibly(queryTimeout, unit);
                //long duration = System.currentTimeMillis() - beginTime;
                //if (2000 < duration) {
                //System.err.println("Query took " + duration + " ms");
                //}
                futureAction.onSuccess();
            } catch (Exception e) {
                insertErrors++;
                futureAction.onFailure(e, line);
                if (maxInsertErrors <= insertErrors) {
                    futureAction.onTooManyFailures();
                    return false;
                }
            }
        }
        futures.clear();
        return true;
    }

    public boolean cleanup() {
        return purgeFutures();
    }

    public long getNumInserted() {
        return numInserted;
    }
}
