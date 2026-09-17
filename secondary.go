package secondary 

import (
	"crypto/rand"
	"encoding/hex" // display results directly to users
	"encoding/json"
	"fmt"
	"log" // For tracking errors/status message
	"net/http"

	"github.com/gorilla/websocket"
)

// MessageType distinguishes the routing of incoming message
type MessageType string

const ( MsgBroadcast MessageType ="broadcast")

// RequestMessage represents the structure of an incoming JSON payload [15]
type RequestMessage struct {
	Type   MessageType `json:"type"`
	Data   string      `json:"data"`
	Client *Client     `json:"-"` // Tracks which client sent the message [15]
}

// Response Message represents the sructure of an outgoing broadcast
type ResponseMessage struct {
	Type MessageType `json:"type"`
	Data string `json:"data"`
	SenderID string `json:"sender_id"` // ID of the broadcasting client
}

// Client represents a single connected user
type Client struct {
	id   string
	conn *websocket.Conn //WebSocket connection represents the actual communication channel between the server and the client.
	messageChannel chan ResponseMessage
	done           chan struct{}        // Signals when to tear down the write loop [11]
}


// this function is used to initialize a ne client with 10 character random ID
func NewClient(conn *websocket.Conn) *Client {

	b := make([]byte, 5)
	

	if _, err := rand.Read(b); err != nil {
		log.Println("Error generating random ID:", err)
	}

	fmt.Println("this is byte format of Client ID ===>", b)

	id := hex.EncodeToString(b)

	return &Client{
		id:   id,
		conn: conn,
		messageChannel: make(chan ResponseMessage, 64), // Safe queue of 64 messages  , it can hold up to 64 messages 
		done:           make(chan struct{}),
	}

}

// server holds the actual clients and handles synchoronization
type Server struct {
	clients map[string]*Client   // clinets are stored in a map to have a Key-value pair , syntax:map[key-type]value-type
	joinChannel chan *Client
	leaveChannel chan *Client
	broadcastChannel chan RequestMessage 
	

}

// write Loop runs concurrently  & is the sole routine allowed to write to connection 
func (c * Client) writeLoop(){
    
	fmt.Println(" Inside write  Loop ")
	defer func ()  {
		fmt.Println("Write  channel of client closed")
		c.conn.Close()
	}()

	for{

		select {

		case <-c.done:  //exit signal 
			return

		case msg := <-c.messageChannel:
			// write JSON payload safely over websocket 
			if err := c.conn.WriteJSON(msg); err!=nil {
				log.Println("write error for client ",c.id)
				return 
			}
		}
	}
}

// readMessageLoop reads incoming frames from websocket
func (c *Client) readMessageLoop(s * Server){

	fmt.Println(" Inside read Loop ")
	defer func (){
        fmt.Println("read channel of client closed")
		close(c.done)
		s.leaveChannel <- c // queues client to leave server
	}()

	for{

		_,bytes , err := c.conn.ReadMessage()
		if err!=nil {
			log.Println("read connect error for client",c.id,err)
			return 
		}

		
		// convert incoming bytes into structural JSON 
 		var msg RequestMessage
		if err := json.Unmarshal(bytes, &msg); err!=nil {
			log.Println("Unmarshal error ", err)
			continue
		}

		msg.Client = c  // attach sender client
		s.broadcastChannel <- msg   // Send to central server queue

	}
}


// start EventLoop processses additions , removals , braodcasts sequentially 
func ( s* Server) StartEventLoop() {

	fmt.Println(" inside Start Event looop ")
	for{

		select {
		case client := <-s.joinChannel:
				s.clients[client.id]=client
				log.Printf("Client joined: %s (Total clients: %d)\n", client.id, len(s.clients))
				

        case client := <-s.leaveChannel:
				if _,exists := s.clients[client.id];exists{
					delete(s.clients ,client.id)
					log.Printf("Client left: %s (Total clients: %d)\n", client.id, len(s.clients))
			    }
			case msg := <-s.broadcastChannel:
					s.broadcast(msg)
				
		
		}
	}
}

// broadcast copies client targets safely and publishes data in a background routine [20, 21]
func (s *Server) broadcast(msg RequestMessage) {
	// 1. Create a safe, static snapshot copy of active receivers [20-22]
	// This prevents concurrent map modification panic while doing slow I/O [20]
	var targets []*Client
	for _, client := range s.clients {
		// Do not broadcast back to the sender [20, 22]
		if client.id != msg.Client.id {
			targets = append(targets, client) // [22]
		}
	}

	response := ResponseMessage{
		Type:     msg.Type,
		Data:     msg.Data,
		SenderID: msg.Client.id,
	}

	// 2. Offload the network writing to a non-blocking goroutine so we don't hold up 
	// joins/leaves in the event loop [20]
	go func(clients []*Client, resp ResponseMessage) {
		for _, client := range clients {
			select {
			case client.messageChannel <- resp: // Safely queue message for client writer [12]
			default:
				log.Printf("Message queue full for client %s, dropping message\n", client.id)
			}
		}
	}(targets, response)
}

// NewServer intializes and returns a pointer to server
func NewServer() *Server {
	return &Server{
		clients:          make(map[string]*Client),
		joinChannel:      make(chan *Client, 64),
		leaveChannel:     make(chan *Client, 64),
		broadcastChannel: make(chan RequestMessage, 64),
	}
}

// Configure the Gorilla WebSocket upgrader [6]
var upgrader = websocket.Upgrader{
	ReadBufferSize:  512 * 1024, // 512 kilobytes buffer size (in bytes) [6]
	WriteBufferSize: 512 * 1024, // 512 kilobytes [6]
	CheckOrigin: func(r *http.Request) bool {
		// Bypasses origin checks for local development [6]
		return true
	},
}

// handleWS handles incoming HTTP requests and upgrades them to Websocket connection



func main() {
	wsPort := ":3023" // The TCP port our server will listen on
    server := NewServer()

	
	// Spin up the server event loop in its own goroutine [8]
	go server.StartEventLoop()
	
	// define route for our webSocket Connections
	// second function handles incoming HTTP requests and upgrades them to Websocket connection
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		fmt.Println("New WS connection from", conn.RemoteAddr(), "→", conn.LocalAddr())
		if err != nil {
			log.Println("Error on HTTP connection upgrade:", err)
			return
		}

		client := NewClient(conn)
		
		// Send client to serialized join queue [4]
		server.joinChannel <- client
		
		// Spin up independent read/write loops for this individual client [8, 23]
		go client.writeLoop()
		go client.readMessageLoop(server)
	})


	log.Println("Starting server on Port",wsPort)

	// Start the TCP listener
	// If the server fails to start, log.Fatal will automatically log and terminate the process [7]
	if err := http.ListenAndServe(wsPort, nil); err != nil {
		log.Fatal("ListenAndServe error: ", err)
	}

}
