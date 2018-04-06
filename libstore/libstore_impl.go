package libstore

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sort"
	"sync"
	"time"
	// "github.com/cmu440/tribbler/storageserver"
)

//libstor struct
type libstore struct {
	// TODO: implement this!
	mode       LeaseMode
	client     *rpc.Client //to masterServer
	servers    []*wrapped_server
	myHostPort string
	cache      map[string]*cacheValue //key - cache
	queries    map[string]*queries    //key - time list
	cachelock  *sync.Mutex
	querylock  *sync.Mutex
}

//wrapped server with stored conn
type wrapped_server struct {
	node storagerpc.Node
	cli  *rpc.Client
}

// wrapped query timestamp list
type queries struct {
	list *list.List //put time
}

//wrapped cache value
type cacheValue struct {
	leaseEnd   time.Time
	value      string   //for get
	list_value []string //for getLIST
	valid      bool     //if revoke
}

// to help sort
type sortById []*wrapped_server

//help function for sort by struct
func (s sortById) Len() int {
	return len(s)
}

//help function for sort by struct
func (s sortById) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

//help function for sort by struct
func (s sortById) Less(i, j int) bool {
	return s[i].node.NodeID < s[j].node.NodeID
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	ls := &libstore{
		mode:       mode,
		client:     cli,
		myHostPort: myHostPort,
		cache:      make(map[string]*cacheValue),
		queries:    make(map[string]*queries),
		cachelock:  new(sync.Mutex),
		querylock:  new(sync.Mutex),
	}

	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))
	if err != nil {
		return nil, err
	}

	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	for count := 0; count < 6; count++ {
		err = ls.client.Call("StorageServer.GetServers", args, &reply)
		if err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			break
		}
		if count == 5 {
			return nil, errors.New("retry fail")
		}
		time.Sleep(time.Duration(1) * time.Second)

	}
	for _, server := range reply.Servers {
		temp_server := &wrapped_server{
			node: server,
		}
		if server.HostPort == masterServerHostPort {
			temp_server.cli = ls.client
		}
		ls.servers = append(ls.servers, temp_server)
	}
	sort.Sort(sortById(ls.servers))
	go ls.clean_cache()

	return ls, nil
}

//implement Get in libstore_api
func (ls *libstore) Get(key string) (string, error) {
	v, err, want_lease := ls.find_cache(key)
	if err == nil {
		return v.value, nil
	}
	//make sure if want lease
	if ls.mode == Never {
		want_lease = false
	} else if ls.mode == Always {
		want_lease = true
	}

	server := ls.route_request(key)
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: want_lease,
		HostPort:  ls.myHostPort,
	}
	var reply storagerpc.GetReply
	if err := server.cli.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}
	if reply.Status != storagerpc.OK {
		return reply.Value, errors.New("error reply status")
	}
	if reply.Lease.Granted {

		//put in cache
		ls.cachelock.Lock()
		v, ok := ls.cache[key]
		if ok {
			v.valid = true
			v.value = reply.Value
			v.leaseEnd = time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds))
		} else {
			cache_value := &cacheValue{
				valid:    true,
				value:    reply.Value,
				leaseEnd: time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds)),
			}
			ls.cache[key] = cache_value
		}
		ls.cachelock.Unlock()
	}
	return reply.Value, nil
}

//implement Put in libstore_api
func (ls *libstore) Put(key, value string) error {
	server := ls.route_request(key)
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	var reply storagerpc.PutReply
	if err := server.cli.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return errors.New("lib put fail")
}

//implement Delete in libstore_api
func (ls *libstore) Delete(key string) error {
	server := ls.route_request(key)
	args := &storagerpc.DeleteArgs{
		Key: key,
	}
	var reply storagerpc.DeleteReply
	if err := server.cli.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return errors.New("lib delete fail")
}

//implement GetList in libstore_api
func (ls *libstore) GetList(key string) ([]string, error) {
	//find in cache
	v, err, want_lease := ls.find_cache(key)

	if err == nil {
		return v.list_value, nil
	}
	//make sure if want lease
	if ls.mode == Never {
		want_lease = false
	} else if ls.mode == Always {
		want_lease = true
	}

	server := ls.route_request(key)
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: want_lease,
		HostPort:  ls.myHostPort, //just in checkpoint
	}
	var reply storagerpc.GetListReply
	if err := server.cli.Call("StorageServer.GetList", args, &reply); err != nil {
		return nil, err
	}
	if reply.Status != storagerpc.OK {
		return nil, errors.New("lib getlist fail")
	}
	if reply.Lease.Granted {
		//put in cache
		ls.cachelock.Lock()
		v, ok := ls.cache[key]
		if ok {
			v.valid = true
			v.list_value = reply.Value
			v.leaseEnd = time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds))
		} else {
			cache_value := &cacheValue{
				valid:      true,
				list_value: reply.Value,
				leaseEnd:   time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds)),
			}
			ls.cache[key] = cache_value
		}
		ls.cachelock.Unlock()
	}
	return reply.Value, nil
}

//implement RemoveFromList in libstore_api
func (ls *libstore) RemoveFromList(key, removeItem string) error {
	server := ls.route_request(key)

	args := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}
	var reply storagerpc.PutReply
	if err := server.cli.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return errors.New("lib removefromlist fail")
}

//implement AppendToList in libstore_api
func (ls *libstore) AppendToList(key, newItem string) error {
	server := ls.route_request(key)

	args := &storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}
	var reply storagerpc.PutReply
	if err := server.cli.Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return errors.New("lib appendtolist fail")
}

//implement RevokeLease in libstore_api
func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.cachelock.Lock()

	v, ok := ls.cache[args.Key]
	if ok {
		v.valid = false
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	ls.cachelock.Unlock()
	return nil
}

// help functions=======================================================
// use consistent hashing to find slave node
func (ls *libstore) route_request(key string) *wrapped_server {
	hash := StoreHash(key)
	expected := ls.servers[0]
	for _, server := range ls.servers {
		if hash <= server.node.NodeID {
			expected = server
			break
		}
	}

	if expected.cli == nil {
		cli, err := rpc.DialHTTP("tcp", expected.node.HostPort)
		if err != nil {
			fmt.Printf("should not happen dial slave node %v error %v", expected.node.HostPort, err)
		}
		expected.cli = cli
	}
	return expected
}

//add timestamp to querylist and find if already stored hash
//return cachevalue
//if not cached return error
//return if want lease
func (ls *libstore) find_cache(key string) (*cacheValue, error, bool) {
	time_now := time.Now()
	ls.querylock.Lock()
	var query *queries
	query, ok := ls.queries[key]
	if !ok {
		query = &queries{
			list: list.New(),
		}
		ls.queries[key] = query
	}
	query.list.PushBack(time_now)
	for e := query.list.Front(); e != nil; e = e.Next() {
		expired := e.Value.(time.Time).Add(time.Second * time.Duration(storagerpc.QueryCacheSeconds))
		if expired.Before(time_now) {
			query.list.Remove(e)
		}
	}
	count := query.list.Len()
	ls.querylock.Unlock()

	ls.cachelock.Lock()
	v, ok := ls.cache[key]
	if ok && v.valid && time_now.Before(v.leaseEnd) {
		ls.cachelock.Unlock()
		return v, nil, false
	}
	ls.cachelock.Unlock()
	if count >= storagerpc.QueryCacheThresh {
		return nil, errors.New("no cache"), true
	}
	return nil, errors.New("no cache"), false
}

//delete invalid or tiemout cache in every one second
func (ls *libstore) clean_cache() {
	for {
		time.Sleep(time.Duration(1) * time.Second)
		time_now := time.Now()
		ls.cachelock.Lock()
		for k, v := range ls.cache {
			if !v.valid || time_now.After(v.leaseEnd) {
				delete(ls.cache, k)
			}
		}
		ls.cachelock.Unlock()
	}
}
