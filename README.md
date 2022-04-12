# CS6381 Programming Assignment 3

The tests folder contains a file named 'sourcegen.xlsx' - this was used to generate the command source files for mininet consistent with my applications. The tests are essentially the same as for PA1 and 2, however there are fundamental differences from attempting to embed delays and failures. As this required some customization of the generated tests, I limited my output to three cases this time - direct, broker, and one with an explicit fail of the primary broker.

Execution of this system can be accomplished by running zookeeper, then these apps in any order, concurrently:
- registryapp.py (must specify dissemination method if using broker)
- brokerapp.py (if desired)
- subapp.py
- pubapp.py

The system defaults to 1000 publish calls per publisher with random topics and data. The system assumes the zookeeper is at 10.0.0.1 and queries zookeeper for registries and brokers.

Initial start should see all publishers registered with the same registry. They will detect loss of registry and compensate after startup.
Publisher stop requests are not coded to detect loss of registry at this time. There is a known issue where writes to the registry from secondary nodes do not function, though reads seem fine.

For most experiments (unless otherwise noted) my mininet setup was:
mn --topo=single,{num hosts}

# Results
Comparisons between time to broker vs time to subscriber were not examined in this assignment.

Link modifications were not included in this time, as I was more interested in seeing how failures impacted the system. I ran with approximately 30 hosts for each run - a few registries, a few brokers, and several each of publishers and subscribers.

<img src="PA3Graph.png" title="Tail Latencies" caption="Tail Latencies"/>

Each case ran very quickly, in the order of 5 milliseconds on average. The latency curves were very smooth with the broker taking a little longer as expected. I expected to see bumps in the graphs for the broker failures especially. However, there were none! The reason for this is the architecture of my system. When a broker fails, there is nothing transmitted to the subscriber, and therefore no measurable time distortion. The data is simply lost while the subscriber waits to discover the new broker information.

# Conclusion
Interruptions in the flow of data affected the success of data delivery, not the transmission times.

Video demonstration can be found until the middle of July, 2022 at https://vanderbilt.zoom.us/rec/share/ds2Za2bUAD7S8kDvtSXfccfbBwji2kmcIwh-E0AwW38fz56sCfHGiRS01GO_zE_W.KPnl3KWt7HL4cNON?startTime=1649756057000
