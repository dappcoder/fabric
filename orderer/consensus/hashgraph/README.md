### Hashgraph Consensus Orderer Plugin

This is to plug Hashgraph consensus into the Orderer so that HL Fabric runs on Hashgraph consensus (provided by a cluster of Hashgraph nodes).

##### 1. Start hashgraph4orderer

* Follow the steps described [here](https://github.com/dappcoder/hashgraph4orderer)

##### 2. Setup Fabric Dev Environment

* Follow the steps described in [main README](../../../README.md)

##### 3. Start the Orderer
```
cd orderer
go run orderer/main.go
```

##### 4. Test the Orderer Hashgraph Consensus Plugin
```
orderer/sample_clients/broadcast_msg/broadcast_msg
```

##### 5. Check the Logs

* You should see orderer transaction being logged in Hashgraph consoles as a confirmation that consensus in Hashgraph network has been reached.

* In the console of the Orderer (point 3) you should see "HELLO From Hashgraph!" as a confirmation that Hashgraph has pushed the consented transaction back to the Orderer.

* The next step is to write the code that proceeds with including the transaction(s) into blocks.