package surfstore

import (
	context "context"
	"errors"

	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	mu             sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	var fileInfoMap = &FileInfoMap{}
	fileInfoMap.FileInfoMap = m.FileMetaMap
	return fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")
	m.mu.Lock()
	defer m.mu.Unlock()
	fname := fileMetaData.Filename

	if _, err := m.FileMetaMap[fname]; !err {
		m.FileMetaMap[fname] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	} else {

		if fileMetaData.Version-m.FileMetaMap[fname].Version == 1 {
			m.FileMetaMap[fname] = fileMetaData
			return &Version{Version: fileMetaData.Version}, nil
		} else {
			return &Version{Version: -1}, errors.New("version difference should be equal to 1")
		}

	}

}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
