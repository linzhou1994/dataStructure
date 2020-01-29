package com.dataStructure.util.concurrent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ////////////////////////////////////////////////////////////////////
 * //                          _ooOoo_                               //
 * //                         o8888888o                              //
 * //                         88" . "88                              //
 * //                         (| ^_^ |)                              //
 * //                         O\  =  /O                              //
 * //                      ____/`---'\____                           //
 * //                    .'  \\|     |//  `.                         //
 * //                   /  \\|||  :  |||//  \                        //
 * //                  /  _||||| -:- |||||-  \                       //
 * //                  |   | \\\  -  /// |   |                       //
 * //                  | \_|  ''\---/''  |   |                       //
 * //                  \  .-\__  `-`  ___/-. /                       //
 * //                ___`. .'  /--.--\  `. . ___                     //
 * //              ."" '<  `.___\_<|>_/___.'  >'"".                  //
 * //            | | :  `- \`.;`\ _ /`;.`/ - ` : | |                 //
 * //            \  \ `-.   \_ __\ /__ _/   .-` /  /                 //
 * //      ========`-.____`-.___\_____/___.-`____.-'========         //
 * //                           `=---='                              //
 * //      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^        //
 * //         佛祖保佑           永无BUG           永不修改           //
 * //          佛曰:                                                 //
 * //                 写字楼里写字间，写字间里程序员;                 //
 * //                 程序人员写程序，又拿程序换酒钱.                 //
 * //                 酒醒只在网上坐，酒醉还来网下眠;                 //
 * //                 酒醉酒醒日复日，网上网下年复年.                 //
 * //                 但愿老死电脑间，不愿鞠躬老板前;                 //
 * //                 奔驰宝马贵者趣，公交自行程序员.                 //
 * //                 别人笑我忒疯癫，我笑自己命太贱;                 //
 * //                 不见满街漂亮妹，哪个归得程序员?                 //
 * ////////////////////////////////////////////////////////////////////
 *
 * @创建时间: 2020/1/27 23:23
 * @author: linzhou
 * @描述: MyThreadPoolExecutor
 */
public class MyThreadPoolExecutor extends AbstractExecutorService {

    /**
     * 用ctl的高3位表示线程池的状态
     * 低29位表示线程池工作线程的个数
     */
    private final AtomicInteger ctl = new AtomicInteger(RUNNING);
    /**
     * COUNT_BITS=29
     */
    private static final int COUNT_BITS = Integer.SIZE - 3;
    /**
     * 1 << COUNT_BITS = 001000000...后面共29个0
     * CAPACITY = 0001111....一共29个1
     */

    private static final int CAPACITY = (1 << COUNT_BITS) - 1;

    /**
     * 线程池状态运行中（高三位：111）
     * 能接受新的任务，并处理队列中的任务
     * -1 << COUNT_BITS = -1的补码右移29位
     * -1 = 10000000000000000000000000000001 30个0
     * -1的反码：11111111111111111111111111111110
     * -1的补码为反码+1：11111111111111111111111111111111
     * 所以-1 << COUNT_BITS = 11100000000000000000000000000000 29个0
     */
    private static final int RUNNING = -1 << COUNT_BITS;
    /**
     * 高三位为000
     * 关闭状态，不接受新任务，但是处理队列中的任务
     * SHUTDOWN = 0
     */
    private static final int SHUTDOWN = 0 << COUNT_BITS;
    /**
     * 高三位为001
     * 停止状态，拒绝新任务并且抛弃阻塞队列里的任务同时会中断正在处理的任务
     */
    private static final int STOP = 1 << COUNT_BITS;
    /**
     * 010
     * 所有任务都执行完（包含阻塞队列里面任务）当前线程池活动线程为0，将要调用terminated方法
     */
    private static final int TIDYING = 2 << COUNT_BITS;
    /**
     * 011
     * 终止状态。terminated方法调用完成以后的状态
     */
    private static final int TERMINATED = 3 << COUNT_BITS;

    //缓存所有的工作线程
    private final Set<Worker> workerSet = new HashSet<>();
    //线程创建工厂
    private volatile ThreadFactory threadFactory;


    private volatile Integer corePoolSize;

    private volatile Integer maximumPoolSize;

    private volatile Long keepAliveTime;

    private volatile TimeUnit unit;
    /**
     * 用于存放工作线程暂时无法处理的任务
     */
    private final BlockingQueue<Runnable> workQueue;


    public MyThreadPoolExecutor(Integer corePoolSize, Integer maximumPoolSize,
                                Long keepAliveTime, TimeUnit unit,
                                BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.unit = unit;
        this.workQueue = workQueue;
        this.threadFactory = threadFactory;
    }

    public MyThreadPoolExecutor(int corePoolSize,
                                int maximumPoolSize,
                                long keepAliveTime,
                                TimeUnit unit,
                                BlockingQueue<Runnable> workQueue) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                Executors.defaultThreadFactory());
    }


    @Override
    public void shutdown() {

    }

    @Override
    public List<Runnable> shutdownNow() {
        return null;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void execute(Runnable command) {

        if (command == null) {
            throw new NullPointerException();
        }

        int ctlValue = ctl.get();

        //如果线程池的工作线程小于核心线程，则直接创建一个核心线程
        //无论已经创建的核心线程是否处于空闲状态
        if (workerCountOf(ctlValue) < corePoolSize) {
            if (addWorker(command, true)) {
                return;
            } else {
                //如果创建核心线程失败，说明并发提交任务ctl已经被其他线程更改了，所以要重新获取
                ctlValue = ctl.get();
            }
        }
        //如果核心线程已经满了，并且线程池处于运行状态，则加入任务队列中
        if (isRunning(ctlValue) && workQueue.offer(command)) {
            //核心线程允许为0，所以如果核心线程为0时
            // 需要判断当前是否有工作线程，如果没有则创建一个临时线程
            if (workerCountOf(ctlValue) == 0) {
                addWorker(command, false);
            }
        }
        //如果核心线程已经满了，并且无法加入任务队列，则尝试创建一个临时线程
        else if (!addWorker(command, false)) {
            //todo 通知添加失败的监听handler
        }


    }

    /**
     * 添加工作线程
     *
     * @param task 任务
     * @param core 是否为核心线程
     * @return 添加线程是否成功
     */
    private boolean addWorker(Runnable task, boolean core) {

        while (true) {
            int ctlValue = ctl.get();
            //得到当前线程池的状态
            int status = runStateOf(ctlValue);

            //如果线程池为运行中状态，则不会进入这个if
            //如果这个线程不是运行中，但是状态是SHUTDOWN，并且command为空，并且workQueue不为空则不进入这个if
            //除了以上两种情况以外的所有情况都会直接return false
            //所以SHUTDOWN的状态是会继续处理队列中的任务的
            if (isNotRunning(ctlValue) &&
                    !(status == SHUTDOWN
                            && task == null
                            && !workQueue.isEmpty())) {
                return false;
            }

            int workerCount = workerCountOf(ctlValue);
            //获取当前线程的工作线程数
            // 线程池的工作线程数最多不能超过CAPACITY，因为ctl只用了低29位来用于工作线程的计数，所以如果超过CAPACITY就无法统计了
            if (workerCount >= CAPACITY || workerCount >= (core ? corePoolSize : maximumPoolSize)) {
                return false;
            }
            //利用cas使，工作线程+1
            if (compareAndIncrementWorkerCount(ctlValue)) {
                break;
            }
        }

        //===以上为判断是否可以添加一个线程，并且在可以填加的情况下，提前是工作线程+1

        //创建工作线程

        Worker w = new Worker(task);
        Thread t = w.thread;

        boolean rlt = false;

        try {
            if (t != null) {
                int ctlValue = ctl.get();
                //只有runing状态或者(SHUTDOWN状态且无任务的工作线程)可以启动
                if (isRunning(ctlValue) ||
                        (runStateOf(ctlValue) == SHUTDOWN && task == null)) {
                    if (t.isAlive()) {
                        throw new IllegalThreadStateException();
                    }
                    workerSet.add(w);
                    t.start();
                    rlt = true;
                }
            }
        } finally {
            if (!rlt) {
                workerSet.remove(w);
            }
        }


        return rlt;
    }

    /**
     * 判断线程池是否处于运行中
     * 由于除了runing状态其余状态的ctl的第一位都是0
     * 所以只有runing状态ctl才是负数
     * 所以只要ctl<0则为runing
     * SHUTDOWN = 0
     *
     * @return
     */
    private boolean isRunning(int c) {
        return c < SHUTDOWN;
    }

    private boolean isNotRunning(int c) {
        return c >= SHUTDOWN;
    }


    /**
     * clt的高3位为线程池状态，低29位为工作线程数
     * 所以除去clt的高三位后便是工作线程数
     * CAPACITY的值：0001111....一共29个1
     * 所以clt & CAPACITY之后高三位为0而低29位则是clt的原来的低29位
     *
     * @return 工作线程数
     */
    private int workerCountOf(int c) {
        return c & CAPACITY;

    }

    /**
     * 获取线程池的运行状态
     * ~CAPACITY = CAPACITY取反
     *
     * @return
     */
    private int runStateOf(int c) {
        return c & ~CAPACITY;
    }




    private class Worker implements Runnable {
        //当前Worker的第一个任务，可能为null
        private Runnable firstTask;
        //当前Worker所在的线程
        private Thread thread;
        //当前Worker执行完的任务的总数
        volatile long completedTasks = 0L;


        public Worker(Runnable firstTask) {
            this.firstTask = firstTask;
            this.thread = getThread(this);
        }

        @Override
        public void run() {
            runWorker(this);


        }


    }

    private void runWorker(Worker worker) {
        Runnable task = worker.firstTask;
        Thread t = Thread.currentThread();
        try {
            while (task != null || (task = getTask()) != null) {
                //如果当前线程池是STOP状态，则不再接受任务，需要中断线程
                if (runStateOf(ctl.get()) == STOP && !t.isInterrupted()) {
                    t.interrupt();
                }

                try {
                    task.run();
                } catch (Throwable e) {
                    e.printStackTrace();
                } finally {
                    task = null;
                    worker.completedTasks++;
                }

            }
        } finally {
            processWorkerExit(worker);
        }
    }

    private Runnable getTask() {

        while (true) {
            int ctlValue = ctl.get();
            int status = runStateOf(ctlValue);
            /**
             * 如果是SHUTDOWN状态并且队列已经没有任务了
             * 或者状态大于STOP
             * 则可以销毁当前工人了
             */
            if ((status == SHUTDOWN && workQueue.isEmpty()) || status >= STOP) {
                decrementWorkerCount();
                return null;
            }

            try {
                Runnable runnable = workQueue.poll(keepAliveTime,unit);

                if (runnable != null){
                    return runnable;
                }

                int workerCount = workerCountOf(ctlValue);
                //todo
                boolean allowOut = workerCount == 1 ? workQueue.isEmpty() : workerCount > corePoolSize;

                if (allowOut && compareAndDecrementWorkerCount(ctlValue)){
                    return null;
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }

    private void processWorkerExit(Worker worker) {
        workerSet.remove(worker);
    }

    private Thread getThread(Worker worker) {
        return threadFactory.newThread(worker);
    }

    private boolean compareAndIncrementWorkerCount(int expect) {
        return ctl.compareAndSet(expect, expect + 1);
    }

    private boolean compareAndDecrementWorkerCount(int expect) {
        return ctl.compareAndSet(expect, expect - 1);
    }

    private void decrementWorkerCount() {
        while (!compareAndDecrementWorkerCount(ctl.get())) {
        }
    }

    public static void main(String[] args) throws InterruptedException {
        MyThreadPoolExecutor executorService = new MyThreadPoolExecutor(2, 5,
                2L, TimeUnit.SECONDS,
                new SynchronousQueue<>());

        List<Runnable> runnables = new ArrayList<>();

        for (int i = 1;i<=5;i++){
            int finalI = i;
            Runnable r = () -> {
                int n = 3;
                while (n-->0) {
                    System.out.println("r"+ finalI +"===="+Thread.currentThread().getName());
                    try {
                        Thread.sleep(5L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
            runnables.add(r);
        }

        runnables.forEach(r ->{
            executorService.execute(r);
            System.out.println(executorService.workerSet.size());
        });

        Thread.sleep(8000);
        for (Worker worker : executorService.workerSet) {
            System.out.println(worker.thread.getName());

        }

        while (true){
            Thread.sleep(1000);
            System.out.println(executorService.workerSet.size());
        }

    }

}
