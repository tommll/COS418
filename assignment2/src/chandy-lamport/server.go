package chandy_lamport

import (
	"log"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	sync.Mutex
	seenFirstMarker bool // protected by mutex
	//channelMarker   map[string]bool // key = link.src of inboundLinks - protected by mutex
	markerReceived int // protected by mutex

}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		sync.Mutex{},
		false,
		0,
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	server.sim.logger.RecordEvent(server, ReceivedMessageEvent{src, server.Id, message})

	// termination condition: when this server receive (N-1) MarkerMessage
	switch msg := message.(type) {
	case MarkerMessage:
		if server.seenFirstMarker {
			server.sim.logger.RecordEvent(server, SnapshotMessage{src, server.Id, message})
		}

		server.Lock()
		server.markerReceived++
		if server.markerReceived == len(server.outboundLinks) { // receive marker from all channel
			server.sim.NotifySnapshotComplete(server.Id, msg.snapshotId)
			return
		}
		server.Unlock()
	case TokenMessage:
		server.Tokens++
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	// send a marker message out on all outgoing channels
	server.SendToNeighbors(MarkerMessage{snapshotId})
	server.seenFirstMarker = true // set to true no matter seen marker before or not

	// start recording the messages it receives on all incomming channels
	// for src, link := range server.inboundLinks {
	// 	if server.channelMarker[src] == true { // skip channels had received marker
	// 		go func(src string, link *Link) {
	// 			queue := server.inboundLinks[src].events

	// 			for { // listening on queue
	// 				if !queue.Empty() {
	// 					msg := queue.Pop()
	// 					server.HandlePacket(src, msg)

	// 					switch msg.(type) {
	// 					case MarkerMessage: // stop listening from src
	// 						server.channelMarker[src] = true
	// 						break
	// 					default:
	// 					}

	// 				}
	// 			}
	// 		}(src, link)
	// 	}
	// }
}
