package genericsmr

import (
	"bufio"
	"errors"
	"fastrpc"
	"fmt"
	"genericsmrproto"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"rdtsc"
	"state"
	"strings"
)

const CHAN_BUFFER_SIZE = 200000

type RPCPair struct {
	Obj  fastrpc.Serializable
	Chan chan fastrpc.Serializable
}

type Propose struct {
	*genericsmrproto.Propose
	Reply *bufio.Writer
}

type Beacon struct {
	Rid       int32
	Timestamp uint64
}

var addresses = []string{":11111", ":11112", ":11113"}

type Replica struct {
	N              int      // total number of replicas
	Id             int32    // the ID of the current replica
	PeerAddrList   []string // array with the IP:port address of every replica
	PeerReaders    []*bufio.Reader
	PeerWriters    []*bufio.Writer
	Alive          []bool // connection status
	Listener       net.Listener
	PacketListener net.PacketConn

	State *state.State

	ProposeChan chan *Propose // channel for client proposals
	BeaconChan  chan *Beacon  // channel for beacons from peer replicas

	Shutdown bool

	Thrifty bool // send only as many messages as strictly required?
	Exec    bool // execute commands?
	Dreply  bool // reply to client after command has been executed?
	Beacon  bool // send beacons to detect how fast are the other replicas?

	Durable     bool     // log to a stable store?
	StableStore *os.File // file support for the persistent log

	PreferredPeerOrder []int32 // replicas in the preferred order of communication

	rpcTable map[uint8]*RPCPair
	rpcCode  uint8

	Ewma []float64

	OnClientConnect chan bool

	DropRate uint16 // Drop rate for server. Out of 1000.
	isAlive  bool   // Liveness for current server.
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool) *Replica {
	r := &Replica{
		len(peerAddrList),
		int32(id),
		peerAddrList,
		make([]*bufio.Reader, len(peerAddrList)),
		make([]*bufio.Writer, len(peerAddrList)),
		make([]bool, len(peerAddrList)),
		nil,
		nil,
		state.InitState(),
		make(chan *Propose, CHAN_BUFFER_SIZE),
		make(chan *Beacon, CHAN_BUFFER_SIZE),
		false,
		thrifty,
		exec,
		dreply,
		false,
		false,
		nil,
		make([]int32, len(peerAddrList)),
		make(map[uint8]*RPCPair),
		genericsmrproto.GENERIC_SMR_BEACON_REPLY + 1,
		make([]float64, len(peerAddrList)),
		make(chan bool, 100),
		0,
		true}

	var err error

	if r.StableStore, err = os.Create(fmt.Sprintf("stable-store-replica%d", r.Id)); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < r.N; i++ {
		r.PreferredPeerOrder[i] = int32((int(r.Id) + 1 + i) % r.N)
		r.Ewma[i] = 0.0
	}

	return r
}

/* Client API */

func (r *Replica) Ping(args *genericsmrproto.PingArgs, reply *genericsmrproto.PingReply) error {
	if r.isAlive {
		return nil
	}
	return errors.New("server is not alive")
}

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	if r.isAlive {
		return nil
	}
	return errors.New("server is not alive")
}

/* ============= */

/* Client connections dispatcher */
func (r *Replica) WaitForClientConnections() {
	log.Println("Listening on %s", addresses[r.Id])
	r.Listener, _ = net.Listen("tcp", addresses[r.Id])
	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		go r.clientListener(conn)

		r.OnClientConnect <- true
	}
}

func (r *Replica) StartListening() {
	var msgType uint8
	var err error = nil
	var gbeacon genericsmrproto.Beacon
	var gbeaconReply genericsmrproto.BeaconReply
	var n int
	var addr net.Addr
	buffer := make([]byte, 1024)

	// Set all servers to be alive
	for i := 0; i < r.N; i++ {
		r.Alive[i] = true
	}

	r.PacketListener, err = net.ListenPacket("udp", r.PeerAddrList[r.Id])
	if err != nil {
		log.Println("error on listen packet: %s", err.Error())
	}

	for err == nil && !r.Shutdown {
		log.Println("listening")
		n, addr, err = r.PacketListener.ReadFrom(buffer)
		log.Println("read something")
		if err != nil {
			log.Println("packet listener error: ", err.Error())
			break
		}
		reader := strings.NewReader(string(buffer[:n]))
		rid := -1
		for id, serverAddr := range r.PeerAddrList {
			if serverAddr == addr.String() {
				rid = id
				break
			}
		}

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case genericsmrproto.GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			beacon := &Beacon{int32(rid), gbeacon.Timestamp}
			r.BeaconChan <- beacon
			break

		case genericsmrproto.GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			//TODO: UPDATE STUFF
			r.Ewma[rid] = 0.99*r.Ewma[rid] + 0.01*float64(rdtsc.Cputicks()-gbeaconReply.Timestamp)
			log.Println(r.Ewma)
			break

		default:
			if rpair, present := r.rpcTable[msgType]; present {
				obj := rpair.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				rpair.Chan <- obj
			} else {
				log.Println("Error: received unknown message type")
			}
		}
	}
}

func (r *Replica) clientListener(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	var msgType byte //:= make([]byte, 1)
	var err error
	for !r.Shutdown && err == nil {

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case genericsmrproto.PROPOSE:
			prop := new(genericsmrproto.Propose)
			if err = prop.Unmarshal(reader); err != nil {
				break
			}
			r.ProposeChan <- &Propose{prop, writer}
			break

		case genericsmrproto.READ:
			read := new(genericsmrproto.Read)
			if err = read.Unmarshal(reader); err != nil {
				break
			}
			//r.ReadChan <- read
			break

		case genericsmrproto.PROPOSE_AND_READ:
			pr := new(genericsmrproto.ProposeAndRead)
			if err = pr.Unmarshal(reader); err != nil {
				break
			}
			//r.ProposeAndReadChan <- pr
			break

		case genericsmrproto.SET_DROP_RATE:
			dr := new(genericsmrproto.SetDropRate)
			if err = dr.Unmarshal(reader); err != nil {
				break
			}
			r.DropRate = dr.DropRate
			break

		case genericsmrproto.KILL_SERVER:
			r.isAlive = false
			break

		case genericsmrproto.REVIVE_SERVER:
			r.isAlive = true
			break
		}
	}
	if err != nil && err != io.EOF {
		log.Println("Error when reading from client connection:", err)
	}
}

func (r *Replica) RegisterRPC(msgObj fastrpc.Serializable, notify chan fastrpc.Serializable) uint8 {
	code := r.rpcCode
	r.rpcCode++
	r.rpcTable[code] = &RPCPair{msgObj, notify}
	return code
}

type UDPWriter struct {
	conn    net.PacketConn
	address net.Addr
}

type MyAddr struct {
	address string
}

func (a MyAddr) Network() string {
	return "udp"
}

func (a MyAddr) String() string {
	return a.address
}

func (w UDPWriter) Write(p []byte) (n int, err error) {
	conn, err := net.Dial("udp", w.address.String())
	if err != nil {
		log.Println(err)
	}
	defer conn.Close()
	n, err2 := fmt.Fprintf(conn, string(p))
	if err2 != nil {
		log.Println(err2)
		return -1, err2
	}
	return n, nil
}

func (r *Replica) SendMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	log.Println("SENDING MSG: %d", code)
	if r.shouldSendMsg() {
		w := bufio.NewWriter(UDPWriter{r.PacketListener, MyAddr{r.PeerAddrList[peerId]}})
		w.WriteByte(code)
		msg.Marshal(w)
		w.Flush()
	}
}

func (r *Replica) SendMsgNoFlush(peerId int32, code uint8, msg fastrpc.Serializable) {
	if r.shouldSendMsg() {
		w := r.PeerWriters[peerId]
		w.WriteByte(code)
		msg.Marshal(w)
	}
}

func (r *Replica) ReplyPropose(reply *genericsmrproto.ProposeReply, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(genericsmrproto.PROPOSE_REPLY)
	if r.shouldSendMsg() {
		reply.Marshal(w)
		w.Flush()
	}
}

func (r *Replica) ReplyProposeTS(reply *genericsmrproto.ProposeReplyTS, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(genericsmrproto.PROPOSE_REPLY)
	if r.shouldSendMsg() {
		reply.Marshal(w)
		w.Flush()
	}
}

func (r *Replica) SendBeacon(peerId int32) {
	if r.shouldSendMsg() {
		w := bufio.NewWriter(UDPWriter{r.PacketListener, MyAddr{r.PeerAddrList[peerId]}})
		w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON)
		beacon := &genericsmrproto.Beacon{rdtsc.Cputicks()}
		beacon.Marshal(w)
		w.Flush()
	}
}

func (r *Replica) ReplyBeacon(beacon *Beacon) {
	if r.shouldSendMsg() {
		w := bufio.NewWriter(UDPWriter{r.PacketListener, MyAddr{r.PeerAddrList[beacon.Rid]}})
		w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON_REPLY)
		rb := &genericsmrproto.BeaconReply{beacon.Timestamp}
		rb.Marshal(w)
		w.Flush()
	}
}

func (r *Replica) shouldSendMsg() bool {
	rNum := uint16(rand.Intn(1000)) + 1
	return rNum > r.DropRate
}

// updates the preferred order in which to communicate with peers according to a preferred quorum
func (r *Replica) UpdatePreferredPeerOrder(quorum []int32) {
	aux := make([]int32, r.N)
	i := 0
	for _, p := range quorum {
		if p == r.Id {
			continue
		}
		aux[i] = p
		i++
	}

	for _, p := range r.PreferredPeerOrder {
		found := false
		for j := 0; j < i; j++ {
			if aux[j] == p {
				found = true
				break
			}
		}
		if !found {
			aux[i] = p
			i++
		}
	}

	r.PreferredPeerOrder = aux
}
