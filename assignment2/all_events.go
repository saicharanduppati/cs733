package main

import(
	"fmt"
)

type EventType int

type LogEntry struct{
	term 		int
	data 		[]byte
}

const (
    APPEND_EVENT EventType = iota
    TIMEOUT_EVENT
    APPEND_REQ_EVENT
    APPEND_RSP_EVENT
    VOTE_REQ_EVENT
    VOTE_RSP_EVENT
    SEND_EVENT
    COMMIT_EVENT
    ALARM_EVENT
    LOGSTORE_EVENT
)


type Event interface {
	typeOf() 	EventType
	printEvent()
}
//------------------------------------------------------------//
type Append struct {
	data 		[]byte
}
func (a Append) typeOf() EventType{
	return APPEND_EVENT
}
func (a Append) printEvent() {
	fmt.Printf("%#v\n",a)
}

//------------------------------------------------------------//
type Timeout struct {
	t int
	prevState int
}
func (a Timeout) typeOf() EventType{
	return TIMEOUT_EVENT
}
func (a Timeout) printEvent() {
	fmt.Printf("%#v\n",a)
}
func (sm *StateMachine) setAlarm(){
	if sm.state == LEADER {
		sm.actionCh <- Timeout{sm.heartbeatTimeOut,LEADER}
	} else {
		sm.actionCh <- Timeout{sm.electionTimeOut,FOLLOWER}
	}
}


//------------------------------------------------------------//
type AppendEntriesReq struct {
	id      	 int
	term         int
	prevLogIndex int
	prevLogTerm  int
	commitIndex  int
	data      	 []byte
}
func (a AppendEntriesReq) typeOf() EventType{
	return APPEND_REQ_EVENT
}
func (a AppendEntriesReq) printEvent() {
	fmt.Printf("%#v\n",a)
}
func (sm *StateMachine) makeAppendEntriesReq(peerId int) Event{
	return AppendEntriesReq{
		id:				sm.id,
		term:			sm.term,
		prevLogIndex:	sm.iterIndex[peerId],
		prevLogTerm:	sm.log[sm.iterIndex[peerId]].term,
		commitIndex:	sm.commitIndex,
		data:			sm.log[sm.iterIndex[peerId]].data,
	}
}

//------------------------------------------------------------//
type AppendEntriesResp struct {
	id 			int
	term    	int
	success 	bool
}
func (a AppendEntriesResp) typeOf() EventType{
	return APPEND_RSP_EVENT
}
func (a AppendEntriesResp) printEvent() {
	fmt.Printf("%#v\n",a)
}
func (sm *StateMachine) makeAppendEntriesResp(verdict bool) Event{
	return AppendEntriesResp{
		id 			:	sm.id,
		term    	: 	sm.term,
		success 	:	verdict,
	}
}

//------------------------------------------------------------//
type VoteReq struct {
	id          int
	term    	int
	cIndex 		int
	cTerm		int
}
func (a VoteReq) typeOf() EventType{
	return VOTE_REQ_EVENT
}
func (a VoteReq) printEvent() {
	fmt.Printf("%#v\n",a)
}
func(sm *StateMachine) makeVoteReq() Event{
	return VoteReq{
		id       	:	sm.id,
		term    	:	sm.term,
		cIndex 		:	sm.lastLogIndex,
		cTerm		:	sm.log[sm.lastLogIndex].term,
	}
}

//------------------------------------------------------------//
type VoteResp struct {
	id  		int
	term    	int
	success 	bool
}
func (a VoteResp) typeOf() EventType{
	return VOTE_RSP_EVENT
}
func (a VoteResp) printEvent() {
	fmt.Printf("%#v\n",a)
}
func(sm *StateMachine) makeVoteResp(vote bool) Event{
	return VoteResp{
		id       	:	sm.id,
		term    	:	sm.term,
		success		: 	vote,
	}
}

//------------------------------------------------------------//
type Send struct {
	id 		 	int
	event 		Event
}
func (a Send) typeOf() EventType{
	return SEND_EVENT
}
func (a Send) printEvent() {
	fmt.Printf("%#v\n",a)
}
func(sm *StateMachine) sendEvent(peerId int, e Event){
	sm.actionCh <- Send{peerId,e}
}

//------------------------------------------------------------//
type Commit struct {
	index  		int
	data 		[]byte
	err 		int
}
func (a Commit) typeOf() EventType{
	return COMMIT_EVENT
}
func (a Commit) printEvent() {
	fmt.Printf("%#v\n",a)
}
func (sm *StateMachine) makeCommit(commitIndex int) Event{
	return Commit{
		index 	: commitIndex,
		data 	: sm.log[sm.commitIndex].data,
		err 	: 0,
	}
}

//------------------------------------------------------------//
type LogStore struct {
	index 		int
	data 		[]byte
}
func (a LogStore) typeOf() EventType{
	return LOGSTORE_EVENT
}
func (a LogStore) printEvent() {
	fmt.Printf("%#v\n",a)
}
func (sm *StateMachine) logThis(index int, data []byte){
	sm.log[index] = LogEntry{
		term 		: 	sm.term,
		data		:	data,
	}
	sm.	actionCh <- LogStore{index,data}
}