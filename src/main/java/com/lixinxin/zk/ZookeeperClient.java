package com.lixinxin.zk;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ZookeeperClient {
    private static final String ZKServers = "192.168.9.103:2181,192.168.9.104:2181,192.168.9.115:2181";
    ZkClient zkClient = null;

    @Before
    public void init() {
        zkClient = new ZkClient(ZKServers, 10000, 10000, new SerializableSerializer());

        System.out.println("conneted ok!");

    }

    /**
     * 数据的增删改查
     */

    // 创建数据节点到zk中
    @Test
    public void testCreate() {
        String nodeCreated = zkClient.create("/idea/myabc", "hellozk", CreateMode.PERSISTENT);
        //输出创建节点的路径
        System.out.println("created path:" + nodeCreated);
    }

    //判断znode是否存在
    @Test
    public void testExist() {

        boolean exist = zkClient.exists("/idea");
        //返回 true表示节点存在 ，false表示不存在
        System.out.println("节点是否存在:" + exist);

    }

    //获取znode的数据
    @Test
    public void getData() {
        Stat stat = new Stat();
        String znodeData = zkClient.readData("/idea", stat);
        System.out.println("节点数据为：" + znodeData);
        System.out.println(stat);

    }

    //删除znode
    @Test
    public void deleteZnode() {

        //删除单独一个节点，返回true表示成功
        boolean e1 = zkClient.delete("/eclipse");
        System.out.println("是否成功删除单节点：" + e1);
    }

    //删除znode
    @Test
    public void deleteZnodes() {

        //删除含有子节点的节点
        boolean e2 = zkClient.deleteRecursive("/app1");
        System.out.println("是否成功删除含有子节点的节点：" + e2);

    }

    //更新数据
    @Test
    public void updateData() {
        zkClient.writeData("/idea", "update zk data  11112222");
    }

    //订阅节点的信息改变（创建节点，删除节点，添加子节点）
    @Test
    public void childChange() throws InterruptedException {
        /**
         * "/testUserNode" 监听的节点，可以是现在存在的也可以是不存在的
         */
        zkClient.subscribeChildChanges("/idea", new ZKChildListener());
        Thread.sleep(Integer.MAX_VALUE);

    }

    private static class ZKChildListener implements IZkChildListener {
        /**
         * handleChildChange： 用来处理服务器端发送过来的通知
         * parentPath：对应的父节点的路径
         * currentChilds：子节点的相对路径
         */
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {

            System.out.println("父节点路径为：" + parentPath);
            System.out.println("变化的子节点有：" + currentChilds.toString());

        }

    }


    //订阅节点的数据内容的变化
    @Test
    public void dataChange() throws InterruptedException {
        zkClient.subscribeDataChanges("/idea", new ZKDataListener());
        Thread.sleep(Integer.MAX_VALUE);
    }

    private static class ZKDataListener implements IZkDataListener {

        public void handleDataChange(String dataPath, Object data) throws Exception {

            System.out.println("节点" + dataPath + "数据改变成:" + data.toString());
        }

        public void handleDataDeleted(String dataPath) throws Exception {

            System.out.println(dataPath + "节点数据被删除");

        }


    }
}

