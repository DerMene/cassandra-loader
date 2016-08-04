package com.datastax.loader.futures;

public interface FutureAction {
    void onSuccess();

    void onFailure(Throwable t, String line);

    void onTooManyFailures();
}
