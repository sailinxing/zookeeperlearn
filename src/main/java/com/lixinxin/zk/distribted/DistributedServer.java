package com.lixinxin.zk.distribted;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;

public class DistributedServer {
    private static final String ZKServers = "192.168.9.103:2181,192.168.9.104:2181,192.168.9.115:2181";
    private static final String parentNode = "/servers";
    ZkClient zkClient= null;
    public void getConnect(){
        zkClient = new ZkClient(ZKServers,10000,10000,new SerializableSerializer());

        System.out.println("conneted ok!");

    }
    /**
     * 向zk集群注册服务器信息
     *
     * @param hostname
     * @throws Exception
     */
    public void registerServer(String hostname){
        String create =zkClient.create(parentNode+"/server", hostname, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online.." + create);

    }

    /**
     * 业务功能
     *
     *
     */
    public void handleBussiness(String hostname) throws InterruptedException {
        System.out.println(hostname + "start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {

        // 获取zk连接
        DistributedServer server = new DistributedServer();
        server.getConnect();

        // 利用zk连接注册服务器信息
        server.registerServer(args[0]);

        // 启动业务功能
        server.handleBussiness(args[0]);

    }
}
