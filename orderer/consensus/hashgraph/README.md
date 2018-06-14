### Orderer Consensus Plugin

This is to plug Hashgraph consensus into the Orderer.

1. Start hashgraph4orderer as described

2. Setup fabric dev environment as described in main README

3. Start the orderer
```
cd orderer
go run orderer/main.go
```

4. Test the Orderer hashgraph consensus plugin
```
orderer/sample_clients/broadcast_msg/broadcast_msg
```

5. You should see orderer transaction being logged in Hashgraph console and a "HELLO From Hashgraph!" message in the console of the orderer (point 3)