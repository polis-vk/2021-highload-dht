package ru.mail.polis.service.lucas_mbele;

import java.nio.charset.StandardCharsets;
import java.util.Set;

public class RendezvousHashingImpl implements RendezvousHashing {

    private final Set<String> topology;
    private final Murmur2HashImpl hashFunction;
    
    public RendezvousHashingImpl(Set<String> topology) {
        this.hashFunction = new Murmur2HashImpl();
        this.topology = topology;
    }

    @Override
    public String getResponsibleNode(String key) {
        int maxValue = Integer.MIN_VALUE;
        String responsibleNode = "";
        if (this.topology.isEmpty()) {
            return null;
        }
        int score;
        for (String node : topology) {
            String nodeKey = key + node;
            score =  hashFunction.hash32(nodeKey.getBytes(StandardCharsets.UTF_8),key.length());
            if (score > maxValue) {
                responsibleNode = node;
                maxValue = score;
            }
        }
        return responsibleNode;
    }
}
