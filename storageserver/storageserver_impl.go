package storageserver

import (
	"container/list"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
	"time"
)

// valid time duration of lease
const leaseDuration float64 = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

type storageServer struct {
	// TODO: implement this!
	master          *rpc.Client            // the master server in storage system
	servers         []storagerpc.Node      // slice to store all the nodes
	kvStore         map[string]*myItem     // map to store objects
	listMap         map[string]*myItemList // map to store lists of objects
	dialMap         map[string]*rpc.Client // map to store client hold lease
	lockMap         map[string]*sync.Mutex // map to store per key lock
	nodeID          uint32
	numNodes        int         // total number of nodes in the ring
	allRegisterDone chan bool   // signal to tell master server that all slaves have joined
	serversLock     *sync.Mutex // lock used when server join and retrieved
	mapLock         *sync.Mutex // lock used when operating map
}

type myItem struct {
	Value    string               // value to be returned
	leaseMap map[string]time.Time // each key maintain a lease map, key for map is hostport
	revoke   bool                 // flag to show whether revoke called
}

type myItemList struct {
	mylist   *list.List           // list stored when called append/remove/get list
	leaseMap map[string]time.Time // each key maintain a lease map, key for map is hostport
	revoke   bool                 // flag to show whether revoke called
}

// sort interface function used to sort all servers slice before return servers
type sortById []storagerpc.Node

func (s sortById) Len() int {
	return len(s)
}

func (s sortById) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortById) Less(i, j int) bool {
	return s[i].NodeID < s[j].NodeID
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	ss := &storageServer{
		kvStore:         make(map[string]*myItem),
		listMap:         make(map[string]*myItemList),
		dialMap:         make(map[string]*rpc.Client),
		lockMap:         make(map[string]*sync.Mutex),
		numNodes:        numNodes,
		nodeID:          nodeID,
		allRegisterDone: make(chan bool, 1),
		serversLock:     &sync.Mutex{},
		mapLock:         &sync.Mutex{},
	}
	listener, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	if len(masterServerHostPort) == 0 { // master server
		serverInfo := storagerpc.Node{
			HostPort: "localhost:" + strconv.Itoa(port),
			NodeID:   nodeID,
		}
		ss.servers = append(ss.servers, serverInfo) // add master server to slice
		if numNodes > 1 {                           // one or more slaves join
			select {
			case <-ss.allRegisterDone: // wait until all slaves have joined
				sort.Sort(sortById(ss.servers)) // sort server slice first according to nodeId before return
				return ss, nil
			}
		} else { // no other slave nodes
			sort.Sort(sortById(ss.servers)) // sort server slice first according to nodeId before return
			return ss, nil
		}
	} else { // slave server
		slave, err := rpc.DialHTTP("tcp", masterServerHostPort)
		for {
			if err != nil { // retry dial until succeed
				slave, err = rpc.DialHTTP("tcp", masterServerHostPort)
			} else {
				break
			}
		}
		args := &storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{
				HostPort: "localhost:" + strconv.Itoa(port),
				NodeID:   nodeID,
			},
		}
		var reply storagerpc.RegisterReply
		for {
			slave.Call("StorageServer.RegisterServer", args, &reply)
			if reply.Status == storagerpc.OK {
				ss.servers = reply.Servers // reply a slice consisting of all servers
				break
			} else {
				time.Sleep(time.Second) // if not all joined, sleep a second and call again
			}
		}
	}
	sort.Sort(sortById(ss.servers)) // sort server slice first according to nodeId before return
	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.serversLock.Lock()
	if len(ss.servers) == ss.numNodes { // all nodes have already joined
		reply.Status = storagerpc.OK // reply ok and return the slice of nodes
		reply.Servers = ss.servers
	} else {
		flag := false                       // not all nodes joined
		for _, server := range ss.servers { // find out whether the node call the function have joined
			if server.NodeID == args.ServerInfo.NodeID {
				flag = true // this node has already joined, break and return not ready
				break
			}
		}
		if !flag { // this node not join yet
			ss.servers = append(ss.servers, args.ServerInfo) // join this slave server
			if len(ss.servers) == ss.numNodes {              // now all nodes have joined
				reply.Status = storagerpc.OK
				reply.Servers = ss.servers
				ss.allRegisterDone <- true // all slaves have joined
			} else {
				reply.Status = storagerpc.NotReady
			}
		} else {
			reply.Status = storagerpc.NotReady
		}
	}
	ss.serversLock.Unlock()
	return nil // not ready, return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.serversLock.Lock()
	if len(ss.servers) < ss.numNodes { // if still waiting for nodes to join, return nil
		reply.Status = storagerpc.NotReady
	} else { // all nodes joined, return the slice of nodes
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	}
	ss.serversLock.Unlock()
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if ss.CheckNode(args.Key) {
		ss.mapLock.Lock()
		if _, ok1 := ss.lockMap[args.Key]; !ok1 { // if no lock for this key, new and store
			ss.lockMap[args.Key] = &sync.Mutex{}
		}
		ss.mapLock.Unlock()
		ss.lockMap[args.Key].Lock()
		if v, ok := ss.kvStore[args.Key]; ok { // if the key is in map, get the value and return
			reply.Status = storagerpc.OK
			reply.Value = v.Value
			if args.WantLease { // lib ask for a lease
				if !v.revoke { // no revoke calles for the key
					v.leaseMap[args.HostPort] = time.Now() // create a new lease and store the start time
					reply.Lease = storagerpc.Lease{        // return the lease
						Granted:      true,
						ValidSeconds: storagerpc.LeaseSeconds,
					}
				} else { // revoke called, should not grant
					reply.Lease = storagerpc.Lease{
						Granted: false,
					}
				}
			}
		} else {
			reply.Status = storagerpc.KeyNotFound // the key value pair is not in the map
		}
		ss.lockMap[args.Key].Unlock()
	} else {
		reply.Status = storagerpc.WrongServer // hash value is not in the range
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if ss.CheckNode(args.Key) {
		ss.mapLock.Lock()
		if _, ok1 := ss.lockMap[args.Key]; !ok1 { // if no lock for this key, new and store
			ss.lockMap[args.Key] = &sync.Mutex{}
		}
		ss.mapLock.Unlock()
		ss.lockMap[args.Key].Lock()
		if _, ok := ss.kvStore[args.Key]; ok { // the key value pair is in map, find it and delete it
			if len(ss.kvStore[args.Key].leaseMap) > 0 { // the key has lease for several clients
				ss.kvStore[args.Key].revoke = true
				ss.revokeLease(args.Key, ss.kvStore[args.Key].leaseMap, 1) // should revoke first
				ss.kvStore[args.Key].revoke = false
			}
			reply.Status = storagerpc.OK
			delete(ss.kvStore, args.Key)
		} else {
			reply.Status = storagerpc.KeyNotFound
		}
		ss.lockMap[args.Key].Unlock()
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if ss.CheckNode(args.Key) {
		ss.mapLock.Lock()
		if _, ok1 := ss.lockMap[args.Key]; !ok1 { // if no lock for this key, new and store
			ss.lockMap[args.Key] = &sync.Mutex{}
		}
		ss.mapLock.Unlock()
		ss.lockMap[args.Key].Lock()
		if v, ok := ss.listMap[args.Key]; ok { // check if the key already in the map
			reply.Status = storagerpc.OK
			values := make([]string, ss.listMap[args.Key].mylist.Len()) // used to return the value slices
			list := v.mylist
			cnt := 0
			e := list.Front() // put the values in list into the slicess
			for e != nil {
				tmp := e.Value.(string)
				values[cnt] = tmp
				cnt++
				e = e.Next()
			}
			reply.Value = values
			if args.WantLease { // libstore ask for a lease
				if !v.revoke { // revoke not called for this key
					v.leaseMap[args.HostPort] = time.Now() // new lease and store its start time
					reply.Lease = storagerpc.Lease{
						Granted:      true,
						ValidSeconds: storagerpc.LeaseSeconds,
					}
				} else { // revoke called, ungranted
					reply.Lease = storagerpc.Lease{
						Granted: false,
					}
				}
			}
		} else {
			reply.Status = storagerpc.KeyNotFound
		}
		ss.lockMap[args.Key].Unlock()
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if ss.CheckNode(args.Key) {
		ss.mapLock.Lock()
		if _, ok1 := ss.lockMap[args.Key]; !ok1 {
			ss.lockMap[args.Key] = &sync.Mutex{}
		}
		ss.mapLock.Unlock()
		e, ok := ss.kvStore[args.Key]
		if ok && len(e.leaseMap) > 0 { // the key has lease for several clients before
			e.revoke = true
			ss.revokeLease(args.Key, e.leaseMap, 1) // revoke those leases first before modify
			e.revoke = false
		}
		ss.lockMap[args.Key].Lock()
		ss.kvStore[args.Key] = &myItem{ // do modify after revoke finished
			Value:    args.Value,
			leaseMap: make(map[string]time.Time),
			revoke:   false,
		}
		reply.Status = storagerpc.OK
		ss.lockMap[args.Key].Unlock()
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if ss.CheckNode(args.Key) {
		ss.mapLock.Lock()
		if _, ok1 := ss.lockMap[args.Key]; !ok1 {
			ss.lockMap[args.Key] = &sync.Mutex{}
		}
		ss.mapLock.Unlock()
		if _, ok := ss.listMap[args.Key]; !ok { // if no list, create a new list to the key
			ss.listMap[args.Key] = &myItemList{
				mylist:   list.New(),
				leaseMap: make(map[string]time.Time),
				revoke:   false,
			}
		} else {
			if len(ss.listMap[args.Key].leaseMap) > 0 { // the key has lease for several clients before
				ss.listMap[args.Key].revoke = true // revoke those leases first before modify
				ss.revokeLease(args.Key, ss.listMap[args.Key].leaseMap, 2)
				ss.listMap[args.Key].revoke = false
			}
		}
		ss.lockMap[args.Key].Lock()
		e := ss.listMap[args.Key].mylist.Front()
		flag := false
		for e != nil { // check if item already in list, if does, return itemexists
			if e.Value.(string) == args.Value {
				reply.Status = storagerpc.ItemExists
				flag = true
				break
			}
			e = e.Next()
		}
		// this item not exist in list, append it to list
		if !flag {
			reply.Status = storagerpc.OK
			ss.listMap[args.Key].mylist.PushBack(args.Value)
		}
		ss.lockMap[args.Key].Unlock()
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if ss.CheckNode(args.Key) {
		ss.mapLock.Lock()
		if _, ok1 := ss.lockMap[args.Key]; !ok1 {
			ss.lockMap[args.Key] = &sync.Mutex{}
		}
		ss.mapLock.Unlock()
		flag := false
		if e0, ok := ss.listMap[args.Key]; ok {
			if len(e0.leaseMap) > 0 { // the key has lease for several clients before
				e0.revoke = true
				ss.revokeLease(args.Key, e0.leaseMap, 2)
				e0.revoke = false
			}
			ss.lockMap[args.Key].Lock()
			e := e0.mylist.Front()
			for e != nil { // check if item in lists, if not return keynotfound
				if e.Value.(string) == args.Value {
					reply.Status = storagerpc.OK
					e0.mylist.Remove(e)
					flag = true
					break
				}
				e = e.Next()
			}
			ss.lockMap[args.Key].Unlock()
		}
		if !flag { // this item not exist in lis
			reply.Status = storagerpc.ItemNotFound
		}
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) CheckNode(key string) bool {
	hash := libstore.StoreHash(key)
	expected := ss.servers[0] // initial the expected node
	for _, node := range ss.servers {
		if hash <= node.NodeID { // find the first server whose nodeId is bigger than hash value
			expected = node
			break
		}
	}
	if expected.NodeID == ss.nodeID { // check if the given node is the same with expected node
		return true
	} else {
		return false
	}
}

func (ss *storageServer) revokeLease(key string, leaseMap map[string]time.Time, flag int) error {
	for k, v := range leaseMap { // traversal the lease map to revoke each lease
		// set time to cnt till lease expired
		timeout := time.After(time.Duration(leaseDuration-time.Since(v).Seconds()) * time.Second)
		if time.Since(v).Seconds() < leaseDuration { // check if lease still valid
			done := make(chan *rpc.Call, 1) // channel used to indicate that revoke done in libstore
			var cli *rpc.Client
			// each time dial, check if the cli connection already in the cli map
			if _, ok := ss.dialMap[k]; !ok { // if not in map, dial till connection establish
				cc, err := rpc.DialHTTP("tcp", k)
				for err != nil {
					cc, err = rpc.DialHTTP("tcp", k)
				}
				cli = cc // store the cli in map
				ss.dialMap[k] = cli
			} else { // if in the map, use directly
				cli = ss.dialMap[k]
			}
			args := &storagerpc.RevokeLeaseArgs{}
			args.Key = key
			var reply storagerpc.RevokeLeaseReply
			cli.Go("LeaseCallbacks.RevokeLease", args, &reply, done)
			select {
			case <-timeout: // lease expired
				break
			case <-done: // all leases have been revoked
				break
			}
		}
	}
	if flag == 1 { // revoke called in get function
		// clear the leasemap
		ss.kvStore[key].leaseMap = make(map[string]time.Time)
	} else { // revoke called in getlist function
		// clear the leasemap
		ss.listMap[key].leaseMap = make(map[string]time.Time)
	}
	return nil
}
