package kvraft

import "6.5840/kvsrv1/rpc"

type PutArgs struct {
	Key      string
	Value    string
	Version  rpc.Tversion
	ClientId int64
	ReqId    int64
}

type PutReply struct {
	Err rpc.Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	ReqId    int64
}

type GetReply struct {
	Value   string
	Version rpc.Tversion
	Err     rpc.Err
}
