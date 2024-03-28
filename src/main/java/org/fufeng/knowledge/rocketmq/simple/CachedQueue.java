package org.fufeng.knowledge.rocketmq.simple;

import java.util.TreeMap;
import org.apache.rocketmq.common.message.MessageExt;

public class CachedQueue {
    private final TreeMap<Long, MessageExt> msgCachedTable = new TreeMap<>();

    public TreeMap<Long, MessageExt> getMsgCachedTable() {
        return msgCachedTable;
    }
}
