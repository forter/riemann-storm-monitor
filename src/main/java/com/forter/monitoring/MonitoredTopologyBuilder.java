package com.forter.monitoring;

import backtype.storm.topology.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * A {@link backtype.storm.topology.TopologyBuilder} that automatically wraps added bolts with {@link MonitoredBolt} and Spouts with {@link MonitoredSpout}
 */
public class MonitoredTopologyBuilder extends TopologyBuilder {

    final Map<Class<?>, Constructor<? extends IRichBolt>> richBoltConstructorsByInterface;

    public MonitoredTopologyBuilder() {
        richBoltConstructorsByInterface = Maps.newHashMap();
        registerRichBolt(IBasicBolt.class, BasicBoltExecutor.class);
    }

    public void registerRichBolt(Class<?> boltInterface, Class<? extends IRichBolt> richBoltClass) {
        Preconditions.checkArgument(boltInterface.isInterface(), boltInterface.getName() + " must be an interface");
        try {
            Constructor<? extends IRichBolt> constructor = richBoltClass.getConstructor(boltInterface);
            richBoltConstructorsByInterface.put(boltInterface, constructor);
        }
        catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(richBoltClass.getName() + " cannot wrap " + boltInterface.getName(), e);
        }
    }

    @Override
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) {
        return setBolt(id, (Object)bolt, parallelism_hint);
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) {
        return setBolt(id, (Object)bolt, parallelism_hint);
    }

    public BoltDeclarer setBolt(String id, Object bolt, Number parallelism_hint) {
        MonitoredBolt monitoredBolt = null;
        if (bolt instanceof MonitoredBolt) {
            monitoredBolt = (MonitoredBolt) bolt;
        }
        else if (bolt instanceof IRichBolt) {
            monitoredBolt = new MonitoredBolt((IRichBolt) bolt);
        }
        else {

            for (TypeToken<?> boltSuperType : TypeToken.of(bolt.getClass()).getTypes()) {
                if (boltSuperType.getRawType().isInterface()) {
                    Constructor<? extends IRichBolt> cotr = richBoltConstructorsByInterface.get(boltSuperType.getRawType());
                    if (cotr != null) {
                        monitoredBolt = new MonitoredBolt(bolt, cotr);
                    }
                    break;
                }
            }
        }
        Preconditions.checkArgument(monitoredBolt != null,
                bolt.getClass().getName() + " is not supported. " +
                "Register an interface implemented by " + bolt.getClass().getName() + " with " + this.getClass().getName() + "#registerRichBolt()");

        return super.setBolt(id, monitoredBolt, parallelism_hint);
    }

    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) {
        final MonitoredSpout monitoredSpout = (spout instanceof MonitoredSpout) ? (MonitoredSpout)spout: new MonitoredSpout(spout);
        return super.setSpout(id, monitoredSpout, parallelism_hint);
    }
}
