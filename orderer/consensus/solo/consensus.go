/*
SPDX-License-Identifier: Apache-2.0
*/

package solo

import (
	"fmt"
	"time"

	"context"
	"flag"
	"log"

	"net"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/consensus"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/orderer"
	"github.com/op/go-logging"
	ctx "golang.org/x/net/context"
	"google.golang.org/grpc"
)

const pkgLogID = "orderer/consensus/solo"

var (
	logger            *logging.Logger
	hashgraphNodeAddr = flag.String("hashgraph_node_addr", "swirlds-node01:51204", "Hashgraph node address and port")
)

func init() {
	logger = flogging.MustGetLogger(pkgLogID)
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

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New() consensus.Consenter {
	return &consenter{}
}

func (solo *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
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

	msgResp, msgErr := hgClient.Create(hgCtx, &orderer.Transaction{Payload: env.GetPayload()})

	if msgErr != nil {
		log.Println("Could not send message")
	} else {
		log.Println("Sent message. Response: ", msgResp.Accepted)
	}

	return nil
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		configMsg: config,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("exiting")
	}
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
	log.Println("CHAIN: ", ch.support.ChainID())

	if ch.support.ChainID() == "mychannel" {
		// TODO remove hardcoded host
		addressAndPort := "orderer.example.com:52204"
		log.Println("Trying to listen on", addressAndPort)
		lis, err := net.Listen("tcp", addressAndPort)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		log.Println("Listening on", addressAndPort)
		s := grpc.NewServer()
		orderer.RegisterOrdererServiceServer(s, NewOrdererServiceServer(ch))
		// TODO Register reflection service on gRPC servger.
		//reflection.Register(s)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}

	return nil
}

type ordererFeedServer struct {
	ch *chain
}

func NewOrdererServiceServer(ch *chain) orderer.OrdererServiceServer {
	return &ordererFeedServer{ch}
}

func (s *ordererFeedServer) Consensus(ctx ctx.Context, in *orderer.ConsensusTransaction) (*orderer.ConsensusResponse, error) {
	logger.Info("HELLO From Hashgraph!")

	select {
	case s.ch.sendChan <- &message{
		configSeq: uint64(in.Id),
		normalMsg: &cb.Envelope{
			Payload: in.Transaction,
		},
	}:
		return &orderer.ConsensusResponse{Accepted: true}, nil
	}
}
