package com.datastructure.util.lock.zklock;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkDistributeLock2 extends ZkAbstractLock {

    private String beforePath;

    private String currentPath;

    private CountDownLatch countDownLatch;



    protected boolean tryLock(String key) {
        String lockAddress = getLockAddress(this.lockAddress);
        if (StringUtils.isBlank(currentPath)){
            currentPath = client.createEphemeralSequential(
                    getKey(key),key);
        }

        List<String> childDrens =client.getChildren(lockAddress);
        Collections.sort(childDrens);

        if (currentPath.equals(lockAddress+lockSplit+childDrens.get(0))){
            return true;
        }else {
            int wz = Collections.binarySearch(childDrens,currentPath.substring(currentPath.lastIndexOf(lockSplit)+1));

            beforePath = getKey(childDrens.get(wz-1));
        }
        return false;
    }

    protected void waitLock(String key) {

        IZkDataListener listener = new IZkDataListener() {
            public void handleDataChange(String s, Object o) throws Exception {
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }

            public void handleDataDeleted(String s) throws Exception {
            }
        };

        client.subscribeDataChanges(beforePath,listener);

        countDownLatch = new CountDownLatch(1);
        if (client.exists(beforePath)){
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else {
            countDownLatch.countDown();
        }

        client.unsubscribeDataChanges(beforePath,listener);

    }

    public void unLock(String key) {

        client.delete(currentPath);

    }

    public static void main(String[] args) throws InterruptedException {
        ZkDistributeLock2 lock = new ZkDistributeLock2();

        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            public void run() {
                ZkDistributeLock2 lock = new ZkDistributeLock2();

                lock.lock("林周");
                latch.countDown();

                try {
                    Thread.sleep(1000000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        latch.await();
        lock.lock("林周");

        lock.unLock("123");

    }
}
