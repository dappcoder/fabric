package hashgraph

import (
	"github.com/hyperledger/fabric/protos/orderer"
	ctx "golang.org/x/net/context"
	"github.com/hyperledger/fabric/common/flogging"
)

const (
 pkgLogID = "orderer/consensus/hashgraph"
)
var(
	logger = flogging.MustGetLogger(pkgLogID)
)

type ordererFeedServer struct{}

func New() orderer.OrdererFeedServer {
	return &ordererFeedServer{}
}

//Handle(context.Context, *GossipedTransaction) (*HandleResponse, error)
func (s *ordererFeedServer) Consensus(ctx ctx.Context, in *orderer.ConsensusTransaction) (*orderer.ConsensusResponse, error) {
	logger.Info("HELLO From Hashgraph!")
	return &orderer.ConsensusResponse{}, nil
}
