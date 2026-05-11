package imp

import (
	"encoding/json"
	"net"
	"os"
	"time"
	"gopkg.in/yaml.v3"
)

type State string 

const (
	Follower State = "Follower"
	Candidate State = "Candidate"
	Leader State = "Leader"
	LeaderTimeout time.Duration = 5 * time.Second // the leader timeout is 5s, after that we will assume the leader is dead and start a new election
	ElectionTimeout time.Duration = 300 * time.Millisecond // the election timeout is 300ms, after that we will assume the election is failed and start a new election
	CommunicationPort string = ":50000"
	ConfigPath string = "./config.yaml"
)
var Communication net.Conn // this var will hold the tcp connection that all the communication will go through.

type Raft struct {
	  Me            int
      Peers         Config  // holds {"ip:port"} pairs of all nodes in the cluster                                                                                                                    
      State         State
      CurrentTerm   int                                                                                                                         
      VotedFor      int                                                                                                                       
      CurrentLeader int                                                                                                                       
      VotesRecieved []int
      SentLength    []int
      AckedLength   []int   
}

type PeerConfig struct {                                                                            
      ID      int    `yaml:"id"`
      Address string `yaml:"address"`                                                                 
  }                                                                                                 
                                                                                                    
  type Config struct {
      Peers []PeerConfig `yaml:"peers"`
	  HostIP string 	 `yaml:"HostIP"`
  }

// when the node starts newly, this function will propegate a broadcast message to all the other nodes to know whos the 
// leader, if there is a leader, then the node will become a follower, if there is no leader, then the node will become a candidate and start a new election. 
// in the case of that all nodes in the system just started, then all nodes will become candidates and start a new election, in that case, the node with the lowest id will become the leader, and the rest will become followers.
func (r *Raft) StartNode() {
	data, err := os.ReadFile(ConfigPath)
	if err != nil {
		panic("the node can't read the config" + err.Error())
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		print("we can't unmarchal the config" + err.Error())
	}
	
	for i := 0; i < len(cfg.Peers); i++ { // spinning threads to read other nodes data 
		go func(peerAddr string) {
			conn, err := net.DialTimeout("tcp", peerAddr, 1 * time.Second)
			if err != nil {
				print("connection to %s timedout", peerAddr)
			}

			json.NewEncoder(conn).Encode(r) 

			var peerRes Raft
			json.NewDecoder(conn).Decode(&peerRes)
			conn.Close()

			print(peerRes)
			
		}(cfg.Peers[i].Address)
	}

}

func (r *Raft) ReceiveBroadcastMsg(sender int, senderState State, term int, leader int, logLen int) {

}