# Distributed stream processing system

We need to run the failure detector in the protocol, hence we need to run the introducer from MP2 in the background.

## Introducer
* On VM1, run ```python introducer.py &> introducer.log &``` . The introducer listens for machines to connect to it.

## Nimbus
*On VM1 and VM2, run ```python nimbus.py``` to run the master and backup master

## Crane-Client
*On VM3, run the crane client (used to submit jobs) as ```python crane-client.py```.
*At the prompt, use start <config file> to submit a Crane job

##Workers
*On the remaining VMs, run ```python worker.py```. The workers then listen for jobs from the Nimbus