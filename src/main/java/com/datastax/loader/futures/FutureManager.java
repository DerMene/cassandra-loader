package com.datastax.loader.futures;

import com.datastax.driver.core.ResultSetFuture;

public interface FutureManager {
    boolean add(ResultSetFuture future, String line);

    boolean cleanup();

    long getNumInserted();
}
