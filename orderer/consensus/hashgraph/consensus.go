/*
SPDX-License-Identifier: Apache-2.0
*/

package hashgraph

import (
	"time"

	"context"
	"flag"
	"log"

	"github.com/hyperledger/fabric/orderer/consensus"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/orderer"
	"github.com/op/go-logging"
	ctx "golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/hyperledger/fabric/common/flogging"
	"net"
	"fmt"
)

const pkgLogID = "orderer/consensus/hashgraph"

var (
	logger            *logging.Logger
	hashgraphNodeAddr = flag.String("hashgraph_node_addr", "192.168.1.123:51204", "Hashgraph node address and port")
	//hashgraphNodeAddr = flag.String("hashgraph_node_addr", "swirlds-node01:51204", "Hashgraph node address and port")
	ordererServiceServer OrdererFeedServer
)

func init() {
	logger = flogging.MustGetLogger(pkgLogID)
	initGrpc()
}


func initGrpc() {
	// TODO remove hardcoded host
	//addressAndPort := "localhost:52204"
	addressAndPort := "orderer.example.com:52204"
	log.Println("Trying to listen on", addressAndPort)
	lis, err := net.Listen("tcp", addressAndPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Println("Listening on", addressAndPort)
	s := grpc.NewServer()

	ordererServiceServer = NewOrdererServiceServer()
	orderer.RegisterOrdererServiceServer(s,  ordererServiceServer.(orderer.OrdererServiceServer))
	// TODO Register reflection service on gRPC servger.
	//reflection.Register(s)
	go serve(s, lis)
}

func serve(s *grpc.Server, lis net.Listener) {
	serve := s.Serve(lis)
	if err := serve; err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

type consenter struct{}

type chain struct {
	support  consensus.ConsenterSupport
	sendChan chan *message
	exitChan chan struct{}
}

type message struct {
	configSeq uint64
	normalMsg *cb.Envelope
	configMsg *cb.Envelope
}

// Creates a new consenter for the hashgraph consensus scheme.
func New() consensus.Consenter {
	return &consenter{}
}

func (hashgraph *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	return newChain(support), nil
}

func newChain(support consensus.ConsenterSupport) *chain {
	return &chain{
		support:  support,
		sendChan: make(chan *message),
		exitChan: make(chan struct{}),
	}
}

func (ch *chain) Start() {
	go ch.main()
	go ch.hashgraph()
}

func (ch *chain) Halt() {
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

func (ch *chain) WaitReady() error {
	return nil
}

// Order accepts normal messages for ordering
func (ch *chain) Order(env *cb.Envelope, configSeq uint64) error {
	return ch.sendToHashgraphNode(env, configSeq)
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	// TODO return ch.sendToHashgraphNode(config, configSeq)
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		configMsg: config,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

func (ch *chain) sendToHashgraphNode(env *cb.Envelope, configSeq uint64) error {
	conn, err := grpc.Dial(*hashgraphNodeAddr, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Could not connect to Hashgraph node: %v", err)
	}

	log.Println("Connected to Hashgraph node", *hashgraphNodeAddr)

	defer conn.Close()

	hgClient := orderer.NewHashgraphServiceClient(conn)
	log.Println("Created Hashgraph client")
	hgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	msgResp, msgErr := hgClient.Create(hgCtx, &orderer.Transaction{
		ChainID: ch.support.ChainID(),
		Payload: env.GetPayload(),
		Signature: env.GetSignature(),
	})

	if msgErr != nil {
		log.Println("Could not send message!", msgErr)
	} else {
		log.Println("Sent message. Response: ", msgResp.Accepted)
	}

	return nil
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan
}

func (ch *chain) main() {
	var timer <-chan time.Time
	var err error

	for {
		seq := ch.support.Sequence()
		err = nil
		select {
		case msg := <-ch.sendChan:
			if msg.configMsg == nil {
				// NormalMsg
				if msg.configSeq < seq {
					_, err = ch.support.ProcessNormalMsg(msg.normalMsg)
					if err != nil {
						logger.Warningf("Discarding bad normal message: %s", err)
						continue
					}
				}
				batches, _ := ch.support.BlockCutter().Ordered(msg.normalMsg)
				if len(batches) == 0 && timer == nil {
					timer = time.After(ch.support.SharedConfig().BatchTimeout())
					continue
				}
				for _, batch := range batches {
					block := ch.support.CreateNextBlock(batch)
					ch.support.WriteBlock(block, nil)
				}
				if len(batches) > 0 {
					timer = nil
				}
			} else {
				// ConfigMsg
				if msg.configSeq < seq {
					msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
					if err != nil {
						logger.Warningf("Discarding bad config message: %s", err)
						continue
					}
				}
				batch := ch.support.BlockCutter().Cut()
				if batch != nil {
					block := ch.support.CreateNextBlock(batch)
					ch.support.WriteBlock(block, nil)
				}

				block := ch.support.CreateNextBlock([]*cb.Envelope{msg.configMsg})
				ch.support.WriteConfigBlock(block, nil)
				timer = nil
			}
		case <-timer:
			//clear the timer
			timer = nil

			batch := ch.support.BlockCutter().Cut()
			if len(batch) == 0 {
				logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}
			logger.Debugf("Batch timer expired, creating block")
			block := ch.support.CreateNextBlock(batch)
			ch.support.WriteBlock(block, nil)
		case <-ch.exitChan:
			logger.Debugf("Exiting")
			return
		}
	}
}

func (ch *chain) hashgraph() error {
	log.Println("Registering CHAIN: ", ch.support.ChainID())

	ordererServiceServer.RegisterChain(ch)

	return nil
}

type ordererFeedServer struct {
	chains map[string]*chain
}

type OrdererFeedServer interface {
	orderer.OrdererServiceServer
	RegisterChain(ch *chain)
}

func NewOrdererServiceServer() OrdererFeedServer {
	return &ordererFeedServer{
		chains: map[string]*chain{},
	}
}

func (s *ordererFeedServer) RegisterChain(ch *chain) {
	s.chains[ch.support.ChainID()] = ch
}

func (s *ordererFeedServer) Consensus(ctx ctx.Context, in *orderer.ConsensusTransaction) (*orderer.ConsensusResponse, error) {
	logger.Info("Got consensus transaction from Hashgraph. Payload Bytes:", len(in.Payload), "TxSeq:", in.TxSeq)

	chainById := s.chains[in.ChainID]
	if chainById == nil {
		logger.Warning("Could not find registered chain. ChanID:", in.ChainID)
	} else {
		go send(in, chainById)
	}

	return &orderer.ConsensusResponse{Accepted: true}, nil
}

func send(transaction *orderer.ConsensusTransaction, chain *chain) {
	logger.Info("Sending transaction with Seq:", transaction.TxSeq, "to chain:", chain.support.ChainID())

	if transaction.ConfigMessage {
		select {
		case chain.sendChan <- &message{
			configSeq: uint64(transaction.TxSeq),
			configMsg: &cb.Envelope{
				Payload: transaction.Payload,
				Signature: transaction.Signature,
			},
		}:
		}
	} else {
		select {
		case chain.sendChan <- &message{
			configSeq: uint64(transaction.TxSeq),
			normalMsg: &cb.Envelope{
				Payload: transaction.Payload,
				Signature: transaction.Signature,
			},
		}:
		}
	}
}
