package com.datastructure.util.lock.zklock;

import org.I0Itec.zkclient.IZkDataListener;

import java.util.concurrent.CountDownLatch;

public class ZkDistributeLock extends ZkAbstractLock {

    private CountDownLatch countDownLatch;


    protected boolean tryLock(String key) {
        key = getKey(key);
        try {
            client.createEphemeral(key);
            return true;
        } catch (RuntimeException e) {
            e.printStackTrace();
            return false;
        }

    }


    protected void waitLock(String key) {
        key = getKey(key);
        IZkDataListener listener = new IZkDataListener() {
            public void handleDataChange(String s, Object o) throws Exception {
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }

            public void handleDataDeleted(String s) throws Exception {

            }
        };
        client.subscribeDataChanges(key, listener);

        if (client.exists(key)){
            countDownLatch = new CountDownLatch(1);
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        client.unsubscribeDataChanges(key,listener);


    }

    public void unLock(String key) {
        key = getKey(key);
        if (client!=null){
            client.delete(key);
            client.close();
            System.out.println("zk unLock success key:"+key);
        }

    }

}
