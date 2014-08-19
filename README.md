#riemann-storm-monitor
=========

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

## Features ##
The riemann-storm-monitor supplies various monitoring tools - 
* **Events and Functions Throughput measuring** - the usage of this feature requires the user's bolt/spout classes to implement the interface "IEventSenderAware".
  The implementation is as such - 
  ```java
  public class MockSpout implements IEventSenderAware{
        private IEventSender es;

        @Override
        public void setEventSender(IEventSender es) {
            this.es = es;
            es.
        }
        ...
  ```

*IEvenSender events (from [IEventSender.java]("https://github.com/forter/riemann-storm-monitor/blob/prod/src/main/java/com/forter/monitoring/IEventSender.java")
```java
public interface IEventSender {
    void sendThroughputEvent(String service, String messageId);
    void sendException(Throwable t, String service);
    void sendException(String description, String service);
    void sendLatency(long latency, String service, Throwable er);
    void sendEvent(String description, String service, double metric);
}
```

* **Bolt / Spout latency monitoring** - the usage of this feature is automatic.
