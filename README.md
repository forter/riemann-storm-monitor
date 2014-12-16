#riemann-storm-monitor
==========

The riemann-storm-monitor is a library that acts as a riemann agent.
The library supplies various tools meant for monitoring of storm topologies.


## Usage ##
The main classes of the library are the MonitoredBolt and MonitoredSpout classes. These classes act as wrappers to other bolts and spouts.
These classes are used when defining a topology - 

```java
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("testMockSpout",new MonitoredSpout(new MockSpout()), 1);
builder.setBolt("testMockBolt", new MonitoredBolt(new MockBolt()), 1).localOrShuffleGrouping("testMockSpout");
```

You should pass configuration for latency map. Set the required values when initializing the topology:
 conf.put("topology.monitoring.latencies.map.maxSize", 500);
 conf.put("topology.monitoring.latencies.map.maxTimeSeconds", 120);
 conf.put("topology.monitoring.latencies.map.maxConcurrency", 4);



## Features ##
The riemann-storm-monitor supplies various monitoring tools - 
* **Events and Functions Throughput measuring** - the usage of this feature requires the user's bolt/spout classes to implement the interface "EventsAware".
  The implementation is as such - 
  ```java
  public class MockSpout implements EventsAware{
        private EventSender es;

        @Override
        public void setEventSender(EventSender es) {
            this.es = es;
        }
        ...
        private foo() {
          es.sendEvent("sent each time foo is called","foo called",1,"mytag1","mytag2");
        }
  ```

* **IEvenSender events (from [EventSender.java](src/main/java/com/forter/monitoring/EventSender.java))

* **Bolt / Spout latency monitoring** - the usage of this feature is automatic.
