package com.lixinxin.zk.distribted;

import com.lixinxin.zk.ZookeeperClient;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

import java.util.ArrayList;
import java.util.List;

public class DistributedClient {
    private static final String ZKServers = "192.168.9.103:2181,192.168.9.104:2181,192.168.9.115:2181";
    private static final String parentNode = "/servers";
    // 注意:加volatile的意义何在？
    private volatile List<String> serverList;
    ZkClient zkClient = null;

    public void getConnect() {
        zkClient = new ZkClient(ZKServers, 10000, 10000, new SerializableSerializer());

        System.out.println("conneted ok!");

    }

    /**
     * 获取服务器信息列表
     */
    public void getServerList() throws Exception {

        // 获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zkClient.getChildren(parentNode);
        zkClient.subscribeChildChanges(parentNode, new ZKChildListener());
        // 先创建一个局部的list来存服务器信息
        List<String> servers = new ArrayList<String>();
        for (String child : children) {
            // child只是子节点的节点名
            String data = zkClient.readData(parentNode + "/" + child);
            servers.add(data);
        }
        // 把servers赋值给成员变量serverList，已提供给各业务线程使用
        serverList = servers;
        //打印服务器列表
        System.out.println("服务器列表为：" + serverList);

    }

    public class ZKChildListener implements IZkChildListener {
        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            getServerList();
        }

    }

    /**
     * 业务功能
     *
     * @throws InterruptedException
     */
    public void handleBussiness() throws InterruptedException {
        System.out.println("client start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }


    public static void main(String[] args) throws Exception {

        // 获取zk连接
        DistributedClient client = new DistributedClient();
        client.getConnect();
        // 获取servers的子节点信息（并监听），从中获取服务器信息列表
        client.getServerList();

        // 业务线程启动
        client.handleBussiness();

    }

}
