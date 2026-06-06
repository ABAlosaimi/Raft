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
	ConfigPath string = "./config.yaml"
)

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
		panic("the node can't read the configuration file:" + err.Error())
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		print("we can't unmarchal the configuration data to set the peers and host IP:" + err.Error())
	}
	
	// spinning up threads to read other nodes data on the begging of a node (it act as a hi to other nodes)
	for i := 0; i < len(cfg.Peers); i++ { 
		go func(peerAddr string) {
			conn, err := net.DialTimeout("tcp", peerAddr, 5 * time.Second)
			if err != nil {
				print("connection to %s failed", peerAddr)
			}

			err = json.NewEncoder(conn).Encode(r) 
			if err != nil {
				print("we can't send to %s", peerAddr)
			}

			var peerRes Raft
			json.NewDecoder(conn).Decode(&peerRes)
			conn.Close()

			print(peerRes)
			
		}(cfg.Peers[i].Address)
	}

}

func (r *Raft) OnStartlistener() {
	data, err := os.ReadFile(ConfigPath) // the config file should be read once as a const 
	if err != nil {
		print("we can't read the config file to get the host ip")
	}	
	
	var cfg Config 
	yaml.Unmarshal(data, &cfg)

	lis, err := net.Listen("tcp", cfg.HostIP)
	if err != nil {
		print("we can't listen to brodcast messages")
	}
	defer lis.Close()

	var peerRes Raft
	for {
		conn, err := lis.Accept()
		if err != nil { 
				print("we cant't bind a accept to connection")
				continue
			}
		if err := json.NewDecoder(conn).Decode(&peerRes); err != nil {
				print("we cant't read the data from the connection")
          		continue
      		}
		print(peerRes)
	}
}