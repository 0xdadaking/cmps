package hashtree

import (
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/cbergoon/merkletree"
)

// NewFromShardFilenames build file to build hash tree
func NewFromShardFilenames(chunkPath []string) (*merkletree.MerkleTree, error) {
	if len(chunkPath) == 0 {
		return nil, errors.New("Empty data")
	}
	var list = make([]merkletree.Content, 0)
	for i := 0; i < len(chunkPath); i++ {
		f, err := os.Open(chunkPath[i])
		if err != nil {
			return nil, err
		}
		temp, err := io.ReadAll(f)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		list = append(list, HashTreeContent{x: string(temp)})
	}

	//Create a new Merkle Tree from the list of Content
	return merkletree.NewTree(list)
}

func NewFromShards[T io.Reader](shards []T) (*merkletree.MerkleTree, error) {
	if shards == nil || len(shards) == 0 {
		return nil, errors.New("empty shards")
	}
	mtcs := make([]merkletree.Content, len(shards))
	for i, s := range shards {
		data, err := io.ReadAll(s)
		if err != nil {
			return nil, err
		}
		mtcs[i] = HashTreeContent{x: string(data)}
	}
	return merkletree.NewTree(mtcs)
}
