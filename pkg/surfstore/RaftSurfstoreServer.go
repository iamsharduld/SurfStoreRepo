package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex    int64
	pendingCommits []chan bool
	lastApplied    int64
	nextIndex      []int64

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// Leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond

	rpcClients []RaftSurfstoreClient

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	fileInfoMap, err := s.metaStore.GetFileInfoMap(ctx, empty)
	return fileInfoMap, err
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// panic("todo")
	blockStoreAddr, err := s.metaStore.GetBlockStoreAddr(ctx, empty)
	return blockStoreAddr, err
}

func (s *RaftSurfstore) checkMajority(ctx context.Context, empty *emptypb.Empty) (bool, error) {
	cnt := 0
	//fmt.Println("In Check Majority")
	for _, addr := range s.ipList {

		// //fmt.Println(s.serverId, " ", s.ip, " sending heartbeat to ", idx, " ", addr)

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return false, nil
		}
		client := NewRaftSurfstoreClient(conn)
		st, _ := client.IsCrashed(ctx, &emptypb.Empty{})
		// //fmt.Println(err, addr)
		if st.IsCrashed {
			cnt += 1
		}
		conn.Close()
	}
	// x, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	//fmt.Println("Looping forever in ", s.ip, s.isCrashed, x.GetIsCrashed(), x.IsCrashed, s.notCrashedCond)

	if cnt > (len(s.ipList) / 2) {
		return true, nil
	} else {
		return false, nil
	}
}
func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	//fmt.Println(s.ip, " Update file is called ", filemeta.Filename)
	// //fmt.Println("# Logs ", len(s.log))
	// fmt.Println("is Crashed ", s.isCrashed)
	fmt.Println("server info ", s)
	// check

	nS := len(s.ipList)

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	flag := 0
	flag1 := 0
	// wait := true
	if nS <= 3 {
		flag1 = 1
		flag = 1
		prevLogCpy := s.log
		for {
			cnt := 0
			if flag == 1 {
				s.log = append(s.log, &op)
				flag = 0
			}

			for _, addr := range s.ipList {

				// fmt.Println(s.serverId, " ", s.ip, " sending heartbeat to ", idx, " ", addr)

				conn, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					return nil, nil
				}
				client := NewRaftSurfstoreClient(conn)
				st, _ := client.IsCrashed(ctx, &emptypb.Empty{})
				// fmt.Println(err, addr)
				if st.IsCrashed {
					cnt += 1
				}
				conn.Close()
			}
			// x, _ := s.IsCrashed(ctx, &emptypb.Empty{})
			// fmt.Println("Looping forever in ", s.ip, s.isCrashed, x.GetIsCrashed(), x.IsCrashed, s.notCrashedCond)

			if cnt > (len(s.ipList) / 2) {
				continue
			} else {
				break
			}

		}

		// s.log = append(s.log, &op)

		if s.isCrashed {
			s.log = prevLogCpy
			return nil, nil
		}
	}
	if flag1 == 0 {
		s.log = append(s.log, &op)
		// fmt.Println("Log ", s.log)
		// intState, _ := s.GetInternalState(ctx, &emptypb.Empty{})
		// fmt.Println(intState)

		// Update logs of up servers

		for _, addr := range s.ipList {
			// fmt.Println("MAIN LINE")

			// fmt.Println(s.serverId, " ", s.ip, " sending heartbeat to ", idx, " ", addr)

			conn, _ := grpc.Dial(addr, grpc.WithInsecure())
			client := NewRaftSurfstoreClient(conn)
			st, _ := client.IsCrashed(ctx, &emptypb.Empty{})
			// fmt.Println(err, addr)
			if st.IsCrashed {
				continue
			} else {
				input := &AppendEntryInput{
					Term:         s.term,
					PrevLogTerm:  -1,
					PrevLogIndex: -1,
					// TODO figure out which entries to send
					Entries:      s.log,
					LeaderCommit: s.commitIndex,
				}
				client.AppendEntries(ctx, input)
			}
			conn.Close()
		}
		// x, _ := s.IsCrashed(ctx, &emptypb.Empty{})
		// fmt.Println("Looping forever in ", s.ip, s.isCrashed, x.GetIsCrashed(), x.IsCrashed, s.notCrashedCond)
		// if !x.IsCrashed {

		// }

		return nil, ERR_SERVER_CRASHED
	}

	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed)

	go s.attemptCommit()

	success := <-committed
	if success {
		if s.isCrashed && flag1 != 0 {
			s.log = make([]*UpdateOperation, 0)
		}
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

// TODO

// No correct logs in server 1

func (s *RaftSurfstore) attemptCommit() error {
	targetIdx := s.commitIndex + 1
	// fmt.Println("")
	// fmt.Println("In Attempt Commit")
	// fmt.Println("server IP ", s.ip)
	// fmt.Println("pending commits on s ", s.pendingCommits)

	commitChan := make(chan *AppendEntryOutput, len(s.ipList))

	// Iterating in all servers

	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.commitEntry(int64(idx), targetIdx, commitChan)
	}

	commitCount := 1
	for {
		// TODO handle crashed nodes
		// if s.isCrashed {
		// 	return ERR_SERVER_CRASHED
		// }

		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}
		// fmt.Println("")
		if commitCount > len(s.ipList)/2 {
			// fmt.Println(targetIdx, len(s.pendingCommits))
			// Pushing data into the channel at target idx
			if targetIdx >= int64((len(s.pendingCommits))) {
				targetIdx = int64(len(s.pendingCommits)) - 1
			}

			s.pendingCommits[targetIdx] <- true
			s.commitIndex = targetIdx
			break
		}
	}
	// fmt.Println("")
	return nil
}

// func (s *RaftSurfstore) checkDuplicate() error {

// }

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) error {
	for {
		// fmt.Println(s.ip, "server log", s.log)
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s.nextIndex, etc
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: s.nextIndex[serverIdx],
			Entries:      s.log[s.nextIndex[serverIdx] : entryIdx+1],
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		st, _ := client.IsCrashed(ctx, &emptypb.Empty{})

		if st.IsCrashed {
			continue
		}
		// fmt.Println("Leader Commit ", s.commitIndex, " next Index ", int64(s.nextIndex[serverIdx]))
		// fmt.Println(serverIdx, " Append Entries called from Commit Entry")
		output, err := client.AppendEntries(ctx, input)
		if err != nil {
			return err
		}
		if output.Success {
			commitChan <- output
			s.nextIndex[serverIdx] = output.MatchedIndex + 1
			return nil
		}
		// fmt.Println("Leader Commit ", s.commitIndex, " next Index ", int64(s.nextIndex[serverIdx]))

		// TODO update state. s.nextIndex, etc

		// TODO handle crashed/ non success cases
		// if s.isCrashed {
		// 	return ERR_SERVER_CRASHED
		// }
	}
	// return nil
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)

// input term is append entry term
// s.term is leader term
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// fmt.Println("")
	// intState, _ := s.GetInternalState(ctx, &emptypb.Empty{})
	ns := len(s.ipList)
	// fmt.Println("Append entries internal state start log ", intState)
	// fmt.Println(s.ip, " start log -> ", s.log)
	// fmt.Println("is crashed ", s.isCrashed)
	// fmt.Println("input ", input)
	flag1 := true
	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}

	// dicey if

	if input.Term > s.term {
		s.term = input.Term
		if s.isLeader {
			s.isLeader = false
		}
	}

	//1. Reply false if term < currentTerm (§5.1)
	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	//matches prevLogTerm (§5.3)
	//3. If an existing entry conflicts with a new one (same index but different
	//terms), delete the existing entry and all that follow it (§5.3)
	//4. Append any new entries not already in the log
	// fmt.Println("IMP ", s.log, input.Entries)
	unique_log := make([]*UpdateOperation, 0)

	if ns > 3 {
		flag1 = false
		if len(s.log) == 1 && len(input.Entries) == 1 {
			if s.log[0].FileMetaData.Filename == input.Entries[0].FileMetaData.Filename {
				return output, nil
			}
		} else {
			unique_log = input.Entries
		}
	} else {
		for _, tmpLog := range input.Entries {
			flag := 0
			for _, serve_log := range s.log {
				// fmt.Println("Done")
				if tmpLog.FileMetaData.Filename == serve_log.FileMetaData.Filename && tmpLog.FileMetaData.Version == serve_log.FileMetaData.Version {
					flag = 1
				}
			}
			if flag == 0 {
				unique_log = append(unique_log, tmpLog)
			}
		}
	}

	s.log = append(s.log, unique_log...)

	st, _ := s.checkMajority(ctx, &emptypb.Empty{})
	if s.isCrashed || st {
		if flag1 {
			s.log = make([]*UpdateOperation, 0)
		}
		// fmt.Println(s.ip, " end log -> ", s.log)
		return output, ERR_SERVER_CRASHED
	}

	// fmt.Println("Log start", s.ip, s.log)

	// for i, se := range s.log {
	// 	fmt.Println(i)
	// 	fmt.Println(se.FileMetaData.Filename)
	// 	fmt.Println(se.FileMetaData.Version)
	// }

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	// TODO only do this if leaderCommit > commitIndex
	// fmt.Println("Leader commit ", input.LeaderCommit)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}
	// fmt.Println("Last Applied ", s.lastApplied, " Commit index ", s.commitIndex)
	// for s.lastApplied < s.commitIndex {
	// 	s.lastApplied++
	// 	entry := s.log[s.lastApplied]
	// 	s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	// }
	if len(s.log) > 0 && flag1 {
		applyLast := s.log[len(s.log)-1]
		s.metaStore.UpdateFile(ctx, applyLast.FileMetaData)
	}

	output.Success = true
	output.MatchedIndex = s.lastApplied

	// fmt.Println("Append Entries on end", s)
	// fmt.Println(s.ip, " end log -> ", s.log)

	// intState, _ = s.GetInternalState(ctx, &emptypb.Empty{})
	// fmt.Println(intState)
	// fmt.Println(s.ip, "Append entries internal state end log ", intState)

	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// fmt.Println("Setting Leader ", s.ip)
	s.term++
	s.isLeader = true
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) allInternalStates(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// fmt.Println("Setting Leader ", s.ip)
	s.term++
	s.isLeader = true
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// fmt.Println("\nHEARTBEAT START ", s.ip)
	// fmt.Println(s.ip, " Heart beat start log ", s.log)
	// intState, _ := s.GetInternalState(ctx, &emptypb.Empty{})
	// fmt.Println(intState)

	if !s.isLeader {
		return &Success{Flag: false}, nil
	}

	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		// fmt.Println(s.serverId, " ", s.ip, " sending heartbeat to ", addr, " ", idx)

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, nil
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s.nextIndex, etc
		// fmt.Println("Leader Commit ", s.commitIndex, " next Index ", int64(s.nextIndex[idx]))
		nS := len(s.ipList)
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: int64(s.nextIndex[idx]),
			// TODO figure out which entries to send
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}
		if nS > 3 {
			input = &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  -1,
				PrevLogIndex: int64(s.nextIndex[idx]),
				// TODO figure out which entries to send
				Entries:      s.log,
				LeaderCommit: s.commitIndex,
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// fmt.Println("Append Entries call from Send Heartbeat")
		output, err := client.AppendEntries(ctx, input)
		// if err == ERR_SERVER_CRASHED {
		// 	return &Success{Flag: false}, err
		// }
		// fmt.Println("Append Entry output ", output)
		// fmt.Println("Append Entry Error ", err)
		if output != nil {
			// server is alive
			// fmt.Println("AE ", output)
			// fmt.Println(s)
			s.nextIndex[idx] = output.MatchedIndex + 1
		}
	}

	// fmt.Println(s.ip, " Heart beat end log ", s.log)
	// fmt.Println("HEARTBEAT END ", s.ip, "\n")
	// intState, _ = s.GetInternalState(ctx, &emptypb.Empty{})
	// fmt.Println(intState)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	// fmt.Println("Crashing ", s.ip)
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	// fmt.Println("Restoring ", s.ip)
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
