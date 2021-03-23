package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
)

var (
	startIndex int
)

type ProposeId struct {
	Ballot int
	Id     int
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	n_p       map[int]ProposeId   // seq num, higest prepare ever seen with every seq
	n_a       map[int]ProposeId   // seq num, higest accept seen with every seq
	val       map[int]interface{} // propose value, higest accept seen
	value     map[int](interface{})
	doneValue int
	//MaxValue interface{}
}

type AskPrepareMsg struct {
	N          ProposeId
	Seq        int
	StartIndex int
}

type ReplyPrepareMsg struct {
	N           ProposeId
	Seq         int
	Val         interface{}
	Ack_prepare int // 0:defalut, 1:true, 2:false
}

type AskAcceptMsg struct {
	N   ProposeId
	Seq int
	Val interface{}
}

type ReplyAcceptMsg struct {
	N          ProposeId
	Ack_accept bool
}

type DecideMsg struct {
	N   ProposeId
	Seq int
	Val interface{}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//log.SetFlags(log.Lshortfile | log.LstdFlags)
			fmt.Println("paxos Dial() failed: ", err1, "srv is:", srv, "name is:", name, "args is:", args, "reply is:", reply)
			//log.Fatalln("paxos call error")

		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func greaterProposeId(a, b ProposeId) bool {
	if (a.Ballot > b.Ballot) || ((a.Ballot == b.Ballot) && a.Id > b.Id) {
		return true
	}
	return false
}

func equalProposeId(a, b ProposeId) bool {
	return a.Ballot == b.Ballot && a.Id == b.Id
}

func (px *Paxos) Decide(in DecideMsg, out *int) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if val, ok := px.value[in.Seq]; ok {
		if val != in.Val {
			log.Fatalln("px.me is:", px.me, "wocao, unconsistent, unbelieveable,  error", "DecideMsg", in, "px.value", px.value[in.Seq], "px.n_p", px.n_p, "px.val", px.val[in.Seq], "px.n_a", px.n_a)
		}
	}
	px.value[in.Seq] = in.Val
	*out = 1
	Debug(0, px.me, " has finished handle decide:", in, px.value, "out is:", *out)
	return nil
}

func (px *Paxos) HandleAccept(in AskAcceptMsg, out *ReplyAcceptMsg) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if greaterProposeId(in.N, px.n_p[in.Seq]) || equalProposeId(in.N, px.n_p[in.Seq]) {
		px.n_p[in.Seq] = in.N
		px.n_a[in.Seq] = in.N
		px.val[in.Seq] = in.Val
		*out = ReplyAcceptMsg{in.N, true}
	} else {
		*out = ReplyAcceptMsg{px.n_p[in.Seq], false}
	}
	Debug(0, "px.me is:", px.me, "handel accept:", in, out)
	return nil
}

func (px *Paxos) HandlePrepare(in AskPrepareMsg, out *ReplyPrepareMsg) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	// if _, ok := px.value[in.seq]; ok {

	// 	return nil
	// }
	if greaterProposeId(in.N, px.n_p[in.Seq]) || equalProposeId(in.N, px.n_p[in.Seq]) {
		*out = ReplyPrepareMsg{px.n_p[in.Seq], in.Seq, px.val[in.Seq], 1}
		px.n_p[in.Seq] = in.N
	} else {
		*out = ReplyPrepareMsg{px.n_p[in.Seq], in.Seq, nil, 2}
	}
	Debug(0, "px.me:", px.me, "handel prepare:AskPrepareMsg is", in, "ReplyPrepareMsg is", out)
	//return false, nil
	return nil
}

func (px *Paxos) SendPrepare(seq int, n ProposeId, v interface{}) {
	Debug(0, "px.me is:", px.me, "send prepare is:", seq, n, v)
	px.mu.Lock()
	defer px.mu.Unlock()
	actualVal := v // value get from other node whne prepare
	ballotMax := 0 // ballot get from other node whne prepare
	replyPreNMax := ProposeId{0, px.me}
	chPrepare := make(chan int)
	for i := 0; i < len(px.peers); i++ {
		go func(i int) {
			// if i == px.me {
			// 	chPrepare <- 1
			// 	return
			// }
			var reply ReplyPrepareMsg
			var ask AskPrepareMsg
			ask.N = n
			ask.Seq = seq
			if i == px.me {
				px.HandlePrepare(ask, &reply)
				//ask = ReplyPrepareMsg{px.n_p[in.Seq], in.Seq, px.val[in.Seq], 1}
			} else {
				if ok := call(px.peers[i], "Paxos.HandlePrepare", ask, &reply); !ok {
					//log.SetFlags(log.Lshortfile | log.LstdFlags)
					//log.Fatalln("call HandlePrepare error")
					fmt.Println(px.me, "call HandlePrepare error", "px.unreliable is", px.unreliable)
				}
			}

			if reply.Ack_prepare == 1 && reply.Val != nil {
				if greaterProposeId(reply.N, replyPreNMax) {
					replyPreNMax = reply.N
					actualVal = reply.Val
				}
			} else if reply.Ack_prepare == 2 {
				ballotMax = reply.N.Ballot
			}
			chPrepare <- reply.Ack_prepare
			Debug(0, "px.me is:", px.me, "after prepare:send to ", i, "ask is:", ask, "reply is", reply)
		}(i)
	}

	chAccept := make(chan int)

	go func() {
		startAcceptFlag := 0 // only begin accpet phase once
		prepareOk := 0
		for {
			c := <-chPrepare
			if c == 1 {
				prepareOk++
				Debug(0, "preparea ok num:", prepareOk)
				if prepareOk > len(px.peers)/2 && startAcceptFlag == 0 {
					for i := 0; i < len(px.peers); i++ {
						go func(i int) {
							// if i == px.me {
							// 	chAccept <- 1
							// 	return
							// }
							var ask AskAcceptMsg
							var reply ReplyAcceptMsg
							ask.N = n
							ask.Seq = seq
							ask.Val = actualVal
							Debug(0, "px.me", px.me, "before accpet:send to", i, ask, reply)
							if i == px.me {
								px.HandleAccept(ask, &reply)
							} else {
								call(px.peers[i], "Paxos.HandleAccept", ask, &reply)
							}
							if reply.Ack_accept {
								chAccept <- 1
							} else {
								chAccept <- 0
							}
							//chAccept <- (reply.Ack_accept == true)
							Debug(0, "px.me", px.me, "after accpet:send to", i, ask, reply)
						}(i)
					}
					//break
					startAcceptFlag = 1
				}
			} else if c == 2 {
				// when prepare ask recieve reject:
				//fmt.Println("----------------------------22222---------------------")
				n = ProposeId{ballotMax + 1, px.me}
				Debug(0, "next round prepare", n)
				px.n_p[seq] = n
				px.n_a[seq] = n
				go px.SendPrepare(seq, n, v)
				//fmt.Println("----------------------------333---------------------")
				break
			}
		}
	}()

	go func() {
		acceptOk := 0
		startDecideFlag := 0
		for {
			c := <-chAccept
			if c == 1 {
				acceptOk++
				if acceptOk > len(px.peers)/2 && startDecideFlag == 0 {
					for i := 0; i < len(px.peers); i++ {
						go func(i int) {
							var ask DecideMsg
							var reply int
							ask.N = n
							ask.Seq = seq
							ask.Val = actualVal
							if i == px.me {
								px.Decide(ask, &reply)
								return
							}
							call(px.peers[i], "Paxos.Decide", ask, &reply)
							Debug(0, "px.me", px.me, "send decide to", i, "after decide:", ask, reply)
						}(i)
					}
					startDecideFlag = 1
				}
			} else if c == 2 {
				break
			}
		}
	}()

}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	Debug(0, "the min of all paxos:", px.Min())
	if seq < px.Min() {
		return
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	Debug(0, "when start, the Propose id me, and seq, and v, ----has hnown: px.n_p, px.n_a, :", px.me, seq, v, "px.n_p is:", px.n_p, "px.n_a is:", px.n_a, "px.value is:", px.value)
	if px.n_a[seq].Ballot == 0 && px.n_p[seq].Ballot == 0 {
		go px.SendPrepare(seq, ProposeId{1, px.me}, v)
	} else {
		var n ProposeId
		if px.n_p[seq].Ballot > px.n_a[seq].Ballot {
			n = ProposeId{px.n_p[seq].Ballot + 1, px.me}
		} else {
			n = ProposeId{px.n_a[seq].Ballot + 1, px.me}
		}
		px.n_p[seq] = n
		px.n_a[seq] = n
		go px.SendPrepare(seq, n, v)
	}

}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if px.doneValue < seq {
		px.doneValue = seq
	}
	min := px.Min()
	for key := range px.value {
		if key < min {
			delete(px.value, key)
			delete(px.val, key)
			delete(px.n_p, key)
			delete(px.n_a, key)
		}
	}
	// golang delete map, not actual free memory, see https://www.cnblogs.com/Jun10ng/p/12688285.html
	if len(px.value) == 0 {
		px.n_a = make(map[int]ProposeId)
		px.n_p = make(map[int]ProposeId)
		px.val = make(map[int]interface{})
		px.value = make(map[int](interface{}))
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	max := 0
	for key := range px.value {
		if key > max {
			max = key
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) MinAssitant(_ struct{}, out *int) error {
	//min := int(^uint(0) >> 1)
	px.mu.Lock()
	defer px.mu.Unlock()
	*out = px.doneValue
	return nil
}
func (px *Paxos) Min() int {
	// You code here.
	min := 0
	res := int(^uint(0) >> 1)
	for i := 0; i < len(px.peers); i++ {
		call(px.peers[i], "Paxos.MinAssitant", *(new(struct{})), &min)
		if res > min {
			res = min
		}
	}
	return res + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.

	if seq < px.Min() {
		Debug(0, "the seq is less than px.Min")
		return false, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	val, ok := px.value[seq]
	if !ok {
		Debug(0, "the seq has not been decided yet")
		return false, nil
	}
	return true, val
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.n_a = make(map[int]ProposeId)
	px.n_p = make(map[int]ProposeId)
	px.val = make(map[int]interface{})
	px.value = make(map[int](interface{}))
	px.doneValue = -1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
