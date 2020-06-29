package com.datastructure.util.lock.zklock;

import com.datastructure.util.lock.Lock;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

public abstract class ZkAbstractLock implements Lock {

    private final String zkAddress = "127.0.0.1:2181";

    protected final String lockAddress = "lock";

    protected final String lockSplit = "/";

    protected ZkClient client = new ZkClient(zkAddress);

    public ZkAbstractLock() {
        String lockAddress = getLockAddress(this.lockAddress);
        if (!client.exists(lockAddress)){
            client.createPersistent(lockAddress);
        }
    }

    public void lock(String key) { 

        if (tryLock(key)) {
            System.out.println("zk lock success key:" + key);
        } else {
            waitLock(key);

            lock(key);
        }
    }

    protected abstract void waitLock(String key);

    protected abstract boolean tryLock(String key);

    protected String getKey(String key) {
        if (StringUtils.isBlank(key)) {
            return getLockAddress(lockAddress);
        } else {
            return getLockAddress(lockAddress, key);
        }
    }

    protected String getLockAddress(String... packNames) {
        StringBuilder sb = new StringBuilder();
        for (String packName : packNames) {
            if (!packName.substring(0, 1).equals(lockSplit)) {
                sb.append(lockSplit);
            }
            sb.append(packName);
        }
        return sb.toString();
    }
}
