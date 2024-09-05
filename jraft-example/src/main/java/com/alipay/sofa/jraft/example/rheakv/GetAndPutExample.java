/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.example.rheakv;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;
import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

/**
 *
 * @author jiachun.fjc
 */
public class GetAndPutExample {

    private static final Logger LOG = LoggerFactory.getLogger(GetAndPutExample.class);

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        put2(client.getRheaKVStore());
        client.shutdown();
    }

    public static void put(final RheaKVStore rheaKVStore) {
        final CompletableFuture<byte[]> f1 = rheaKVStore.getAndPut(writeUtf8("getAndPut"), writeUtf8("getAndPutValue"));
        LOG.info("Old value: {}", readUtf8(FutureHelper.get(f1)));

        final CompletableFuture<byte[]> f2 = rheaKVStore.getAndPut("getAndPut", writeUtf8("getAndPutValue2"));
        LOG.info("Old value: {}", readUtf8(FutureHelper.get(f2)));

        final byte[] b1 = rheaKVStore.bGetAndPut(writeUtf8("getAndPut1"), writeUtf8("getAndPutValue3"));
        LOG.info("Old value: {}", readUtf8(b1));

        final byte[] b2 = rheaKVStore.bGetAndPut(writeUtf8("getAndPut1"), writeUtf8("getAndPutValue4"));
        LOG.info("Old value: {}", readUtf8(b2));
    }


    public static void flush(){
        // 初始化 RPC 服务
        CliClientService cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());
// 获取路由表
        RouteTable rt = RouteTable.getInstance();
// raft group 集群节点配置
        Configuration conf =  JRaftUtils.getConfiguration(Configs.ALL_NODE_ADDRESSES);
// 更新路由表配置
        rt.updateConfiguration(String.valueOf(Configs.raftGroupId), conf);
// 刷新 leader 信息，超时 10 秒，返回成功或者失败
        boolean success = false;
        try {
            success = rt.refreshLeader(cliClientService, Configs.raftGroupId, 10000).isOk();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        if(success){
            // 获取集群 leader 节点，未知则为 null
            PeerId leader = rt.selectLeader(Configs.raftGroupId);
            System.out.println("leader:"+leader);
        }
    }

    public static void put2(final RheaKVStore rheaKVStore) {
        rheaKVStore.put("name","xiaochangbai".getBytes());

        new Thread(()->{
            while (true){
                try {
                    byte[] bytes = rheaKVStore.bGet("name");

                    RouteTable rt = RouteTable.getInstance();
                    PeerId leader = rt.selectLeader(Configs.raftGroupId);
                    System.out.println("leader:"+leader);
                    System.out.println("["+leader+"]read:"+new String(bytes));
                }catch (Exception e){
                    e.printStackTrace();
                    flush();
                }
                try {
                    Thread.sleep(1000*1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        new Thread(()->{
            while (true){
                Random random = new Random();
                int nextInt = random.nextInt(100000);
                String newName = "xiaochangbai"+nextInt;
                try {
                    if(rheaKVStore.bPut("name",newName.getBytes())){
                        System.out.println("最新的名字:"+newName);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    flush();
                }
                try {
                    Thread.sleep(1000*1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        try {
            System.in.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
