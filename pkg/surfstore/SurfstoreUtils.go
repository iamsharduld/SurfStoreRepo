package surfstore

import (
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
)

func ClientSync(client RPCClient) {
	syncDir, readErr := ioutil.ReadDir(client.BaseDir)
	if readErr != nil {
		log.Println("Read client base directory error: ", readErr)
	}

	directoryMap := make(map[string]os.FileInfo)
	for _, file := range syncDir {
		directoryMap[file.Name()] = file
	}

	indexFilePath := client.BaseDir + "/index.txt"
	if _, indexFileErr := os.Stat(indexFilePath); os.IsNotExist(indexFileErr) {
		file, _ := os.Create(indexFilePath)
		defer file.Close()
	}

	indexMap := make(map[string]int)

	indexFile, _ := ioutil.ReadFile(indexFilePath)
	indexLines := strings.Split(string(indexFile), "\n")

	// s

	localIndexFileInfoMap, err := LoadMetaFromMetaFile(client.BaseDir)
	var i = 0
	for k := range localIndexFileInfoMap {
		indexMap[k] = i
		i += 1
	}

	log.Println(indexMap)

	// e

	if err != nil {
		log.Println("Error occured")
	}

	clientFileInfoMap := getClientFileInfoMap(client, localIndexFileInfoMap, &indexMap, directoryMap, &indexLines)
	serverFileInfoMap := make(map[string]*FileMetaData)

	getErr := client.GetFileInfoMap(&serverFileInfoMap)
	if getErr != nil {
		log.Println(getErr)
	}

	// log.Println(localIndexFileInfoMap)
	// log.Println(serverFileInfoMap)
	// log.Println(clientFileInfoMap)

	for fname, metadata := range clientFileInfoMap {
		if _, ok := serverFileInfoMap[fname]; ok {
			serverFileMetaData := serverFileInfoMap[fname]
			clientFileMetaData := metadata
			if clientFileMetaData.Version == serverFileMetaData.Version {
				// log.Println("Here12446")

				for i, v := range clientFileMetaData.BlockHashList {
					if v != serverFileMetaData.BlockHashList[i] {
						metadata, err := updateClientFile(client, *serverFileMetaData, clientFileInfoMap)
						if err != nil {
							log.Println(metadata)
						}
						break
					}
				}

				continue
			} else if (clientFileMetaData.Version > serverFileMetaData.Version) ||
				(clientFileMetaData.Version == serverFileMetaData.Version) {
				log.Println("Here124")

				err := updateServerFile(client, clientFileMetaData, clientFileInfoMap)
				if err == nil {
					// log.Println(metadata)

					clientFileInfoMap[fname] = metadata
				}
			} else {
				log.Println("Here1246")
				metadata, err := updateClientFile(client, *serverFileMetaData, clientFileInfoMap)
				if err != nil {
					log.Println(metadata)
				}
			}
		} else {

			err := upload(client, metadata, clientFileInfoMap)
			if err != nil {
				log.Println(metadata)
			}
		}
	}

	for fname, serverFileMetaData := range serverFileInfoMap {
		if _, ok := clientFileInfoMap[fname]; !ok {
			log.Println("1")

			log.Println("Here I am")
			metadata, err := download(client, fname, *serverFileMetaData)
			if err != nil {
				log.Println(err)
			}
			localIndexFileInfoMap[fname] = &metadata
			clientFileInfoMap[fname] = metadata

		}
	}

	WriteMetaFile(clientFileInfoMap, client.BaseDir)
}

func convArrayOfStr(arr []string) string {
	hashStr := ""
	for i, hash := range arr {
		hashStr += hash
		if i != len(arr)-1 {
			hashStr += " "
		}
	}
	return hashStr
}

func getClientFileInfoMap(client RPCClient, indexFileInfoMap map[string]*FileMetaData, indexMap *map[string]int, dirMap map[string]os.FileInfo, indexLines *[]string) map[string]FileMetaData {
	tmpMap := make(map[string]FileMetaData)
	DeleteLogic(indexFileInfoMap, tmpMap, dirMap)

	for fileName, f := range dirMap {
		if fileName == "index.txt" {
			continue
		}

		file, oErr := os.Open(client.BaseDir + "/" + fileName)
		if oErr != nil {
			log.Println(oErr)
		}
		fileSize := f.Size()
		numBlock := int(math.Ceil(float64(fileSize) / float64(client.BlockSize)))

		var info FileMetaData

		if fileMetaData, ok := indexFileInfoMap[fileName]; ok {
			changed, hashList := getHashList(file, *fileMetaData, numBlock, client.BlockSize)
			info.Filename = fileName
			info.Version = fileMetaData.Version
			hashStr := ""
			for i, hash := range hashList {
				info.BlockHashList = append(info.BlockHashList, hash)
				hashStr += hash
				if i != len(hashList)-1 {
					hashStr += " "
				}
			}
			if changed {
				info.Version = fileMetaData.Version + 1

			}
		} else {
			var metaData FileMetaData
			_, hashList := getHashList(file, metaData, numBlock, client.BlockSize)
			info.Filename = fileName
			info.Version = 1
			hashStr := ""
			for idx, hash := range hashList {
				info.BlockHashList = append(info.BlockHashList, hash)
				hashStr += hash
				if idx != len(hashList)-1 {
					hashStr += " "
				}
			}
			info.BlockHashList = hashList
		}

		tmpMap[fileName] = info
	}
	return tmpMap
}

func DeleteLogic(indexFileInfoMap map[string]*FileMetaData, tmpMap map[string]FileMetaData, dirMap map[string]os.FileInfo) {
	for fileName, metadata := range indexFileInfoMap {
		if _, ok := dirMap[fileName]; !ok {
			if len(metadata.BlockHashList) == 1 && metadata.BlockHashList[0] == "0" {
				tmpMap[fileName] = *indexFileInfoMap[fileName]
			} else {
				tmpMap[fileName] = *(&FileMetaData{Filename: metadata.Filename, Version: metadata.Version + 1, BlockHashList: []string{"0"}})
			}
		}
	}
}

func getHashList(file *os.File, fileMetaData FileMetaData, numBlock int, blockSize int) (bool, []string) {
	hashList := make([]string, numBlock)
	var mFlag bool
	for i := 0; i < numBlock; i++ {
		buf := make([]byte, blockSize)
		n, e := file.Read(buf)
		if e != nil {
			log.Println(e)
		}
		buf = buf[:n]

		hashCode := GetBlockHashString(buf)
		hashList[i] = hashCode
		if i >= len(fileMetaData.BlockHashList) || hashCode != fileMetaData.BlockHashList[i] {
			mFlag = true
		}
	}
	if numBlock != len(fileMetaData.BlockHashList) {
		mFlag = true
	}
	return mFlag, hashList
}

func upload(client RPCClient, fileMetaData FileMetaData, clientFileInfoMap map[string]FileMetaData) error {
	var err error

	filePath := client.BaseDir + "/" + fileMetaData.Filename
	if _, e := os.Stat(filePath); os.IsNotExist(e) {
		err = client.UpdateFile(&fileMetaData, &fileMetaData.Version)
		if err != nil {
			log.Println(err)
			serverFileInfoMap := make(map[string]*FileMetaData)
			redownloadNewVersion(serverFileInfoMap, client, fileMetaData, clientFileInfoMap)
		}
		return err
	}

	file, openErr := os.Open(filePath)
	if openErr != nil {
		log.Println(openErr)
	}

	defer file.Close()

	f, _ := os.Stat(filePath)
	numBlock := int(math.Ceil(float64(f.Size()) / float64(client.BlockSize)))
	var succ bool
	var add string

	for i := 0; i < numBlock; i++ {
		var block Block
		block.BlockData = make([]byte, client.BlockSize)
		n, readErr := file.Read(block.BlockData)
		if readErr != nil && readErr != io.EOF {
			log.Println(readErr)
		}
		block.BlockData = block.BlockData[:n]
		block.BlockSize = int32(len(block.BlockData))
		client.GetBlockStoreAddr(&add)
		err = client.PutBlock(&block, add, &succ)

		if err != nil {
			log.Println(err)
		}
	}

	err = client.UpdateFile(&fileMetaData, &fileMetaData.Version)
	if err != nil {
		log.Println(err)
		serverFileInfoMap := make(map[string]*FileMetaData)
		redownloadNewVersion(serverFileInfoMap, client, fileMetaData, clientFileInfoMap)
	}
	return err
}

func redownloadNewVersion(serverFileInfoMap map[string]*FileMetaData, client RPCClient, fileMetaData FileMetaData, clientInfoMap map[string]FileMetaData) error {
	client.GetFileInfoMap(&serverFileInfoMap)
	mdata, err := updateClientFile(client, *serverFileInfoMap[fileMetaData.Filename], clientInfoMap)
	log.Println(mdata)
	return err
}

func updateServerFile(client RPCClient, clientFileMetaData FileMetaData, clientInfoMap map[string]FileMetaData) error {
	err := upload(client, clientFileMetaData, clientInfoMap)

	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func download(client RPCClient, fileName string, fileMetaData FileMetaData) (FileMetaData, error) {
	filePath := client.BaseDir + "/" + fileName
	if _, e := os.Stat(filePath); os.IsNotExist(e) {
		os.Create(filePath)
	} else {
		os.Truncate(filePath, 0)
	}

	if len(fileMetaData.BlockHashList) == 1 && fileMetaData.BlockHashList[0] == "0" {
		err := os.Remove(filePath)
		if err != nil {
			log.Println(err)
		}
		tmp := FileMetaData{Filename: fileMetaData.Filename, Version: fileMetaData.Version, BlockHashList: []string{"0"}}
		return tmp, err
	}

	file, _ := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	defer file.Close()

	hashStr := ""
	var err error
	var blockStoreAddr string
	client.GetBlockStoreAddr(&blockStoreAddr)
	for i, hash := range fileMetaData.BlockHashList {
		var blockData Block
		err = client.GetBlock(hash, blockStoreAddr, &blockData)
		if err != nil {
			log.Println("Get block failed: ", err)
		}

		data := string(blockData.BlockData)

		_, err = io.WriteString(file, data)
		if err != nil {
			log.Println("Write file failed: ", err)
		}

		hashStr += hash
		if i != len(fileMetaData.BlockHashList)-1 {
			hashStr += " "
		}
	}
	tmp := FileMetaData{Filename: fileMetaData.Filename, Version: fileMetaData.Version, BlockHashList: fileMetaData.BlockHashList}
	return tmp, err
}

func updateClientFile(client RPCClient, serverFileMetaData FileMetaData, clientInfoMap map[string]FileMetaData) (FileMetaData, error) {
	mdata, err := download(client, serverFileMetaData.Filename, serverFileMetaData)
	log.Println(mdata)
	clientInfoMap[serverFileMetaData.Filename] = mdata

	if err != nil {
		log.Println("Download failed ", err)
		return mdata, err
	}

	return mdata, nil
}
