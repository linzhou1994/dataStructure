package com.datastructure.util.lock;

public interface Lock {

    void lock(String key);

    void unLock(String key);


}
