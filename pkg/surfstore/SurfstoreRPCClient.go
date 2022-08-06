package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})

	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil || !success.Flag {
		conn.Close()
		return err
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blkHashesOut, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = append(*blockHashesOut, blkHashesOut.Hashes...)

	// var hashes = blkHashesOut.Hashes

	// for _, hsh := range hashes {
	// 	*blockHashesOut = append(*blockHashesOut, hsh)
	// }

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// panic("todo")
	// Check if anything is crashed here.. Recommended to loop and check
	// todo
	leaderAddr, err := surfClient.findLeader()
	if err != nil {
		fmt.Println("Leader not found")
	}

	isMajorityCrashed, err := surfClient.checkMajority()

	if err != nil {
		fmt.Println("Unable to check if majority is crashed")
	}

	if isMajorityCrashed {
		return fmt.Errorf("Majority is crashed")
	}

	conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewRaftSurfstoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	returnServerFileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}

	for k, v := range returnServerFileInfoMap.FileInfoMap {
		(*serverFileInfoMap)[k] = v
	}

	// close the connection
	return conn.Close()

}

func (surfClient *RPCClient) findLeader() (string, error) {
	// panic("todo")
	fmt.Println("Finding Leader")
	leaderAddr := ""
	for i, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return "", err
		}
		c := NewRaftSurfstoreClient(conn)
		st, err := c.GetInternalState(context.Background(), &emptypb.Empty{})
		// perform the call
		if err != nil {
			fmt.Println("Unable to get internal state")
		}

		fmt.Println(surfClient.MetaStoreAddrs[i], addr, st.IsLeader)
		if st.IsLeader {
			leaderAddr = addr
			return leaderAddr, nil
		}
		// close the connection
		conn.Close()
	}

	return "", ERR_NOT_LEADER

}

func (surfClient *RPCClient) checkMajority() (bool, error) {
	// panic("todo")
	fmt.Println("Checking if majority cluster is up")
	cnt := 0
	for i, _ := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return false, err
		}
		c := NewRaftSurfstoreClient(conn)
		st, err := c.IsCrashed(context.Background(), &emptypb.Empty{})
		if err != nil {
			return false, fmt.Errorf("Unable to check if majority is crashed")
		}
		fmt.Println(surfClient.MetaStoreAddrs[i], " is crashed? ", st.IsCrashed)
		if st.IsCrashed {
			cnt += 1
		}
		// close the connection
		conn.Close()
	}

	if cnt > (len(surfClient.MetaStoreAddrs) / 2) {
		return true, nil
	} else {
		return false, nil
	}

}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// panic("todo")

	// only send update files requests to leaders

	leaderAddr, err := surfClient.findLeader()
	if err != nil {
		fmt.Println("Leader not found")
	}

	isMajorityCrashed, err := surfClient.checkMajority()

	if err != nil {
		fmt.Println("Unable to check if majority is crashed")
	}

	if isMajorityCrashed {
		return fmt.Errorf("Majority is crashed")
	}

	// Check if majority cluster is up

	conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewRaftSurfstoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	version, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		return err
	}

	*latestVersion = version.Version

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// connect to the server

	leaderAddr, err := surfClient.findLeader()
	if err != nil {
		fmt.Println("Leader not found")
	}

	isMajorityCrashed, err := surfClient.checkMajority()

	if err != nil {
		fmt.Println("Unable to check if majority is crashed")
	}

	if isMajorityCrashed {
		return fmt.Errorf("Majority is crashed")
	}

	conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewRaftSurfstoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddr = addr.Addr

	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
