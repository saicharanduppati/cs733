package main

import (
	"fmt"
)
const (
	LEADER int = iota
	CANDIDATE
	FOLLOWER
)

type StateMachine struct{
	state 				int
	id 					int
	term 				int
	lastLogIndex		int
	commitIndex			int
	heartbeatTimeOut	int
	electionTimeOut		int
	log 				[]LogEntry
	serverCount			int
	votes 				map[int]int
	iterIndex			map[int]int
	matchIndex			map[int]int
	actionCh			chan Event
	netCh				chan Event 
	timeoutCh			chan Event 
	clientCh 			chan Event
	votedFor			int
}

func min(a,b int) int{
	if a < b {
		return a
	}
	return b
}

func max(a,b int) int{
	if a > b {
		return a
	}
	return b
}

func (sm *StateMachine) clearVotes(){
	for i:=0;i<sm.serverCount;i++ {
		sm.votes[i] = 0
	}
}

func (sm *StateMachine) forVotes() int{
	forvotes := 0
	for i:=0;i<sm.serverCount;i++ {
		if sm.votes[i] == 1 {
			forvotes++
		}
	}
	return forvotes
}

func (sm *StateMachine) againstVotes() int{
	againstVotes := 0
	for i:=0;i<sm.serverCount;i++ {
		if sm.votes[i] == 2{
			againstVotes++;
		}
	}
	return againstVotes;
}

func (sm *StateMachine) leaderPrep(){
	sm.state = CANDIDATE
	sm.term++
	sm.clearVotes()
	sm.votedFor = sm.id
	sm.votes[sm.id] = 1			//for vote
	for i:=0;i<sm.serverCount;i++{
		if i==sm.id{
			continue
		}
		sm.sendEvent(i,sm.makeVoteReq())
	}
}

func (sm *StateMachine) processEvent(event Event){
	switch event.(type) {
	case Append:
		sm.manageAppend(event.(Append))
	case Timeout:
		sm.manageTimeout(event.(Timeout))
	case AppendEntriesReq:
		sm.manageAppendEntriesReq(event.(AppendEntriesReq))
	case AppendEntriesResp:
		sm.manageAppendEntriesResp(event.(AppendEntriesResp))
	case VoteReq:
		sm.manageVoteReq(event.(VoteReq))
	case VoteResp:
		sm.manageVoteResp(event.(VoteResp))
	}
}

func (sm *StateMachine) manageAppend(msg Append){
	switch sm.state {
	case LEADER:
		sm.lastLogIndex++;
		sm.logThis(sm.lastLogIndex,msg.data)
		for i:=0; i < sm.serverCount; i++ {
			if sm.id == i {
				continue;
			}
			sm.sendEvent(i,sm.makeAppendEntriesReq(i))
		}	
	case FOLLOWER, CANDIDATE:
	
	default:
		fmt.Println("Error while manageAppend()")
	}
}

func (sm *StateMachine) manageTimeout(msg Timeout){
	switch sm.state {
	case LEADER:
		if(msg.prevState == LEADER){ //heartbeat time out
			for i:=0; i<sm.serverCount; i++ {
				sm.sendEvent(i,sm.makeAppendEntriesReq(i))
			}
		}
	case CANDIDATE,FOLLOWER:
		if(msg.prevState != LEADER){	//election Timeout
			sm.leaderPrep()
			sm.setAlarm()
		}
	default:
		fmt.Println("Error while manageTimeout()")
	}
}

func (sm *StateMachine) manageAppendEntriesReq(msg AppendEntriesReq){
	switch sm.state {
	case LEADER,CANDIDATE:
		if msg.term >= sm.term {
			sm.state = FOLLOWER
			sm.clearVotes()
			sm.votedFor = -1
			sm.term = msg.term
			sm.setAlarm()	
		} 
		sm.sendEvent(msg.id,sm.makeAppendEntriesResp(false))
	case FOLLOWER:
		sm.setAlarm()
		if msg.term < sm.term {
			sm.sendEvent(msg.id,sm.makeAppendEntriesResp(false))
		} else if sm.lastLogIndex < msg.prevLogIndex || sm.log[sm.lastLogIndex].term != msg.prevLogTerm {
			sm.term = msg.term
			sm.sendEvent(msg.id,sm.makeAppendEntriesResp(false))
		} else{
			sm.term = msg.term
			sm.lastLogIndex = msg.prevLogIndex + 1
			sm.logThis(sm.lastLogIndex,msg.data)
			sm.sendEvent(msg.id,sm.makeAppendEntriesResp(true))
			if msg.commitIndex > sm.commitIndex {
				sm.commitIndex = min(msg.commitIndex,sm.lastLogIndex)
				sm.sendEvent(sm.id,sm.makeCommit(sm.commitIndex))
			}
		}
	default:
		fmt.Println("Error while manageAppendEntriesReq()")
	}
}

func (sm* StateMachine) checkPossibleCommit(peerId int){
	if sm.log[peerId].term == sm.term && sm.commitIndex < sm.matchIndex[peerId] {
		rc := 0
		for i:=0; i<sm.serverCount; i++ {
			if sm.matchIndex[i] >= sm.matchIndex[peerId] {
				rc++
			}
		}
		if rc > sm.serverCount/2 {
			sm.sendEvent(sm.id,sm.makeCommit(sm.matchIndex[peerId]))
		}
	}
}

func (sm* StateMachine) manageAppendEntriesResp(msg AppendEntriesResp){
	switch sm.state{
	case LEADER:
		if msg.success {
			sm.iterIndex[msg.id]++
			sm.matchIndex[msg.id] = sm.iterIndex[msg.id]
			sm.checkPossibleCommit(msg.id)
			if sm.lastLogIndex >= sm.iterIndex[msg.id] {
				sm.sendEvent(msg.id,sm.makeAppendEntriesReq(msg.id))
			}
		} else {
			sm.iterIndex[msg.id] = max(0,sm.iterIndex[msg.id] - 1)
			if sm.lastLogIndex >= sm.iterIndex[msg.id] {
				sm.sendEvent(msg.id,sm.makeAppendEntriesReq(msg.id))
			}
		}
	case CANDIDATE,FOLLOWER:
	default:
		fmt.Println("something wrong at manageAppendEntriesResp()")
	}
}

func (sm *StateMachine) manageVoteReq(msg VoteReq){
	switch sm.state{
	case LEADER,CANDIDATE:
		if msg.term <= sm.term {
			sm.sendEvent(msg.id,sm.makeVoteResp(false))
		} else{
			sm.state = FOLLOWER
			sm.clearVotes()
			sm.term = msg.term
			sm.votedFor = msg.id
			sm.sendEvent(msg.id,sm.makeVoteResp(true))
			sm.setAlarm()
		}
	case FOLLOWER:
		lastLogTerm := sm.log[sm.lastLogIndex].term
		if msg.term <= sm.term {
			sm.sendEvent(msg.id,sm.makeVoteResp(false))
		} else if(sm.votedFor == -1 || sm.votedFor == msg.id) && 
			((msg.cTerm > lastLogTerm) || ((msg.cTerm == lastLogTerm) && (msg.cIndex > sm.lastLogIndex))){
			sm.votedFor = msg.id
			sm.term = msg.term
			sm.sendEvent(msg.id,sm.makeVoteResp(true))
		} else {
			sm.sendEvent(msg.id,sm.makeVoteResp(false))
		}
	}
}

func (sm *StateMachine) manageVoteResp(msg VoteResp){
	switch sm.state{
	case LEADER:

	case CANDIDATE:
		if msg.term > sm.term {
			sm.state = FOLLOWER
			sm.clearVotes()
			sm.term = msg.term
			sm.setAlarm()
		} else {
			if msg.success {
				sm.votes[msg.id] = 1
				if sm.forVotes() > sm.serverCount/2{
					sm.state = LEADER
					for i := 0; i < sm.serverCount ; i++ {
						if i == sm.id {
							continue
						}
						sm.iterIndex[i] = sm.lastLogIndex + 1
						sm.matchIndex[i] = -1
						sm.sendEvent(i,sm.makeAppendEntriesReq(i))
					}
					sm.setAlarm()
				}
			} else{
				sm.votes[msg.id] = 2
				if sm.againstVotes() > sm.serverCount/2{
					sm.state = FOLLOWER
					sm.clearVotes()
					sm.votedFor = -1
					sm.setAlarm()
				}
			}
		}
	case FOLLOWER:

	default:
		fmt.Println("something wrong with manageVoteResp")
	}
}

func (sm *StateMachine) eventLoop() {
	for {
		select {
		case appendMsg := <-sm.clientCh:
			sm.processEvent(appendMsg)

		case peerMsg := <-sm.netCh:
			sm.processEvent(peerMsg)

		case timerMsg := <- sm.timeoutCh:
			sm.processEvent(timerMsg)
		}
	}
}

func newStateMachine(gid,gsize int) *StateMachine{
	return &StateMachine{
		state 				: FOLLOWER,
		id 					: gid,
		term 				: 0,
		lastLogIndex		: -1,
		commitIndex			: -1,
		heartbeatTimeOut	: 1000,
		electionTimeOut		: 500,
		log 				: make([]LogEntry,100),
		serverCount			: gsize,
		votes 				: make(map[int]int),
		iterIndex			: make(map[int]int),
		matchIndex			: make(map[int]int),
		actionCh			: make(chan Event),
		netCh				: make(chan Event),
		timeoutCh			: make(chan Event),
		clientCh 			: make(chan Event),
		votedFor			: -1,
	}
}

func main(){
	sm:= newStateMachine(0,5)
	sm.eventLoop()
}