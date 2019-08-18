package com.lixinxin.zk.zklock;


import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;


import java.util.Collections;
import java.util.List;
import java.util.Random;

public class DistributedClientLock {
    // zookeeper集群地址
    private static final String ZKServers = "192.168.9.103:2181,192.168.9.104:2181,192.168.9.115:2181";
    ZkClient zkClient = null;
    private String groupNode = "locks";
    private String subNode = "sub";
    // 记录自己创建的子节点路径
    private volatile String thisPath;

    public void connectZookeeper() throws Exception {
        zkClient = new ZkClient(ZKServers, 10000, 10000, new SerializableSerializer());
        System.out.println("conneted ok!");
        // 1、程序一进来就先注册一把锁到zk上
        thisPath = zkClient.create("/" + groupNode + "/" + subNode, null, CreateMode.EPHEMERAL_SEQUENTIAL);
        // wait一小会，便于观察
        Thread.sleep(new Random().nextInt(1000));
        // 2、从zk的锁父目录下，获取所有子节点，并且注册对父节点的监听
        List<String> childrenNodes = zkClient.getChildren("/" + groupNode);
        //如果争抢资源的程序就只有自己，则可以直接去访问共享资源
        if (childrenNodes.size() == 1) {
            doSomething();
            thisPath = zkClient.create("/" + groupNode + "/" + subNode, null,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        zkClient.subscribeChildChanges("/" + groupNode, new ZKChildListener());
    }

    public class ZKChildListener implements IZkChildListener {
        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            //获取子节点，并对父节点进行监听
            List<String> childrenNodes = zkClient.getChildren("/" + groupNode);
            String thisNode = thisPath.substring(("/" + groupNode + "/").length());
            // 去比较是否自己是最小id
            Collections.sort(childrenNodes);
            if (childrenNodes.indexOf(thisNode) == 0) {
                //访问共享资源处理业务，并且在处理完成之后删除锁
                doSomething();

                //重新注册一把新的锁
                thisPath = zkClient.create("/" + groupNode + "/" + subNode, null,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
            }
        }

    }

    /**
     * 处理业务逻辑，并且在最后释放锁
     */
    private void doSomething() throws Exception {
        try {
            System.out.println("gain lock: " + thisPath);
            Thread.sleep(2000);
            // do something
        } finally {
            System.out.println("finished: " + thisPath);
            zkClient.delete(this.thisPath);
        }
    }

    public static void main(String[] args) throws Exception {
        DistributedClientLock dl = new DistributedClientLock();
        dl.connectZookeeper();
        Thread.sleep(Long.MAX_VALUE);
    }
}
