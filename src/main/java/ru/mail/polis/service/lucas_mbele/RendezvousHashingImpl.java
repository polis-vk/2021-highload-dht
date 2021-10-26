package ru.mail.polis.service.lucas_mbele;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class RendezvousHashingImpl implements RendezvousHashing{

    private final Set<String> topology;
    private final FNVHashFunctionImpl hashFunction;
    public RendezvousHashingImpl(Set<String>topology){
        this.hashFunction = new FNVHashFunctionImpl();
        this.topology = topology;
    }

    @Override
    public String getResponsibleNode(String key) {
        if (this.topology.isEmpty()){
            return null;
        }
        int maxValue = Integer.MIN_VALUE;
        String responsibleNode = null;
        int score;
        for(String node : topology){
           final String keyNode = key + node;
            score =  hashFunction.hash32(keyNode.getBytes(StandardCharsets.UTF_8));
            if(score > maxValue){
                responsibleNode = node;
                maxValue = score;
            }
        }
        return responsibleNode;
    }
}
