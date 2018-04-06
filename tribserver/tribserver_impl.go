package tribserver

import (
	// "errors"
	// "fmt"

	"encoding/json"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
)

//tribServer struct
type tribServer struct {
	// TODO: implement this!
	ls libstore.Libstore
}

// to help sort
type sortByTime []tribrpc.Tribble

//help function for sort by struct
func (newlist sortByTime) Len() int {
	return len(newlist)
}

//help function for sort by struct
func (newlist sortByTime) Less(i, j int) bool {
	return newlist[i].Posted.UnixNano() < newlist[j].Posted.UnixNano()
}

//help function for sort by struct
func (newlist sortByTime) Swap(i, j int) {
	newlist[i], newlist[j] = newlist[j], newlist[i]
}

//help function to sort tribbles of the user in userlist
//in reverse chronological order
func (ts *tribServer) sortHelper(tribbleList []string) []tribrpc.Tribble {
	var list []tribrpc.Tribble
	for _, postKey := range tribbleList {
		marshaledTribble, err := ts.ls.Get(postKey)
		if err == nil {
			var tribble tribrpc.Tribble
			json.Unmarshal([]byte(marshaledTribble), &tribble)
			list = append(list, tribble)
		}
	}
	if len(list) == 0 {
		return nil
	}
	//sort list
	result := sortByTime(list)
	sort.Sort(sort.Reverse(result))
	if len(result) > 100 {
		result = result[:100]
	}
	return result
}

// help function to verify if the user exist or not
func (ts *tribServer) userExist(key string) bool {
	_, err := ts.ls.Get(key)
	if err != nil {
		return false
	}
	return true
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	// fmt.Println("my trib server")
	newLS, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		return nil, err
	}

	tribServer := new(tribServer)
	tribServer.ls = newLS

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return tribServer, nil
}

//implement CreateUser in tribserver_api
func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	// fmt.Printf("CreateUser %v \n", args.UserID)
	userkey := util.FormatUserKey(args.UserID)
	if ts.userExist(userkey) {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	err := ts.ls.Put(userkey, args.UserID)
	return err
}

//implement AddSubscription in tribserver_api
func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// fmt.Printf("%v AddSubscription of %v \n", args.UserID, args.TargetUserID)
	userkey := util.FormatUserKey(args.UserID)
	targetUserkey := util.FormatUserKey(args.TargetUserID)
	if !ts.userExist(userkey) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if !ts.userExist(targetUserkey) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	key := util.FormatSubListKey(args.UserID)
	err := ts.ls.AppendToList(key, args.TargetUserID) //append to subscript list (formatSublistkey,  subscrip user id)
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

//implement RemoveSubscription in tribserver_api
func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// fmt.Printf("%v RemoveSubscription of %v \n", args.UserID, args.TargetUserID)
	userkey := util.FormatUserKey(args.UserID)
	targetUserkey := util.FormatUserKey(args.TargetUserID)
	if !ts.userExist(userkey) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if !ts.userExist(targetUserkey) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	key := util.FormatSubListKey(args.UserID)
	err := ts.ls.RemoveFromList(key, args.TargetUserID) // possible the list do not exist
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

//implement GetFriends in tribserver_api
func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	// fmt.Printf("GET FRIEND OF %v \n", args.UserID)
	userkey := util.FormatUserKey(args.UserID)
	if !ts.userExist(userkey) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	var friend []string
	key := util.FormatSubListKey(args.UserID)
	subscribList, err := ts.ls.GetList(key)
	if err != nil {
		reply.UserIDs = friend
		reply.Status = tribrpc.OK
		return nil
	}
	for _, user := range subscribList {
		// fmt.Println(user)
		subscribkey := util.FormatSubListKey(user)
		userList, err := ts.ls.GetList(subscribkey)
		if err != nil {
			continue
		}
		for _, subsSubscribe := range userList {
			if subsSubscribe == args.UserID {
				// fmt.Printf("subsub: %v\n", subsSubscribe)
				friend = append(friend, user)
				break
			}
		}
	}
	reply.UserIDs = friend
	reply.Status = tribrpc.OK
	return nil
}

//implement PostTribble in tribserver_api
func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	userkey := util.FormatUserKey(args.UserID)
	if !ts.userExist(userkey) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	posttime := time.Now()
	postTime := posttime.UnixNano()
	tribbleListKey := util.FormatTribListKey(args.UserID)

	//in case hash collision, call formatpostkey again and again
	var postKey string
	for {
		postKey = util.FormatPostKey(args.UserID, postTime)
		_, err := ts.ls.Get(postKey)
		if err != nil {
			break
		}
	}
	ts.ls.AppendToList(tribbleListKey, postKey)
	tribble := &tribrpc.Tribble{
		UserID:   args.UserID,
		Posted:   posttime,
		Contents: args.Contents,
	}
	marshaledTri, _ := json.Marshal(tribble)
	ts.ls.Put(postKey, string(marshaledTri))

	reply.PostKey = postKey
	reply.Status = tribrpc.OK
	return nil
}

//implement DeleteTribble in tribserver_api
func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	userkey := util.FormatUserKey(args.UserID)
	if !ts.userExist(userkey) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	err1 := ts.ls.Delete(args.PostKey)
	key := util.FormatTribListKey(args.UserID)
	err2 := ts.ls.RemoveFromList(key, args.PostKey)
	if err1 != nil || err2 != nil {
		// fmt.Println("either fail in delete or removefromlist")
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

//implement GetTribbles in tribserver_api
func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userkey := util.FormatUserKey(args.UserID)
	if !ts.userExist(userkey) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	key := util.FormatTribListKey(args.UserID)
	tribbleList, err := ts.ls.GetList(key)
	if err != nil {
		reply.Tribbles = nil
	} else {
		reply.Tribbles = ts.sortHelper(tribbleList)
	}
	reply.Status = tribrpc.OK
	return nil
}

//implement GetTribblesBySubscription in tribserver_api
func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userkey := util.FormatUserKey(args.UserID)
	if !ts.userExist(userkey) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	subscripkey := util.FormatSubListKey(args.UserID)
	subscripList, err := ts.ls.GetList(subscripkey)
	if err != nil {
		reply.Status = tribrpc.OK
		reply.Tribbles = nil
		return nil
	}
	var tribblelist []string
	for _, subuser := range subscripList {
		subtribkey := util.FormatTribListKey(subuser)
		tempList, err := ts.ls.GetList(subtribkey)
		if err == nil {
			tribblelist = append(tribblelist, tempList...)
		}
	}

	result := ts.sortHelper(tribblelist)

	reply.Tribbles = result
	reply.Status = tribrpc.OK
	return nil
}
