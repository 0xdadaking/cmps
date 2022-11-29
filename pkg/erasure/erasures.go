package erasure

import (
	"io"
	"log"
	"os"

	"github.com/klauspost/reedsolomon"
	"github.com/pkg/errors"
)

type ShardCreateFunc func(index int) (io.WriteCloser, error)
type ShardOpenFunc func(index int) (r io.ReadCloser, size int64, err error)

type RsecEncoder struct {
	input        io.ReadCloser
	inputSize    int64
	dataShards   int
	parityShards int

	outputCreateFunc ShardCreateFunc
	outputOpenFunc   ShardOpenFunc
}

func (t RsecEncoder) InputSize() int64  { return t.inputSize }
func (t RsecEncoder) DataShards() int   { return t.dataShards }
func (t RsecEncoder) ParityShards() int { return t.parityShards }

func NewRsecEncoder(input io.ReadCloser, inputSize int64, shardCreater ShardCreateFunc, shardOpener ShardOpenFunc) RsecEncoder {
	dataShards, parityShards := reedSolomonRule(inputSize)
	return RsecEncoder{
		input,
		inputSize,
		dataShards,
		parityShards,
		shardCreater,
		shardOpener,
	}
}

func NewRsecEncoderByFile(inputFile *os.File, shardCreater ShardCreateFunc, shardOpener ShardOpenFunc) (*RsecEncoder, error) {
	fstat, err := inputFile.Stat()
	if err != nil {
		return nil, err
	}
	inputSize := fstat.Size()
	dataShards, parityShards := reedSolomonRule(inputSize)
	return &RsecEncoder{
		inputFile,
		inputSize,
		dataShards,
		parityShards,
		shardCreater,
		shardOpener,
	}, nil
}

func (t *RsecEncoder) Encode() error {
	// Create encoding matrix.
	enc, err := reedsolomon.NewStream(t.dataShards, t.parityShards)
	if err != nil {
		return err
	}

	shards := t.dataShards + t.parityShards
	outputs := make([]io.WriteCloser, shards)
	for i := range outputs {
		outputs[i], err = t.outputCreateFunc(i)
		if err != nil {
			return errors.Wrapf(err, "create output for index:%d", i)
		}
	}

	// Split into files.
	data := make([]io.Writer, t.dataShards)
	for i := range data {
		data[i] = outputs[i]
	}
	// Do the split
	err = enc.Split(t.input, data, t.inputSize)
	if err != nil {
		return errors.Wrap(err, "split")
	}

	// Close and re-open the files.
	inputDataShards := make([]io.Reader, t.dataShards)
	for i := range data {
		outputs[i].Close()
		rc, _, err := t.outputOpenFunc(i)
		if err != nil {
			return err
		}
		inputDataShards[i] = rc
		defer rc.Close()
	}

	// Create parity output writers
	parity := make([]io.Writer, t.parityShards)
	for i := range parity {
		parity[i] = outputs[t.dataShards+i]
		defer outputs[t.dataShards+i].Close()
	}

	// Encode parity
	return enc.Encode(inputDataShards, parity)
}

type RsecDecoder struct {
	output       io.Writer
	dataShards   int
	parityShards int

	shardReader  ShardOpenFunc
	shardCreater ShardCreateFunc
}

func NewRsecDecoder(output io.Writer, dataShards, parityShards int, shardReader ShardOpenFunc, shardCreater ShardCreateFunc) RsecDecoder {
	return RsecDecoder{
		output,
		dataShards,
		parityShards,
		shardReader,
		shardCreater,
	}
}

func (t RsecDecoder) buildInputShardReaders() (r []io.Reader, size int64, err error) {
	// Create shards and load the data.
	shards := make([]io.Reader, t.dataShards+t.parityShards)
	for i := range shards {
		f, n, err := t.shardReader(i)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "fail read shard %d", i)
		}
		if n == 0 {
			log.Println("missing shard", i)
			shards[i] = nil
			continue
		}
		shards[i] = f
		size = n
	}
	return shards, size, nil
}

func (t RsecDecoder) Decode() error {
	// Create matrix
	enc, err := reedsolomon.NewStream(t.dataShards, t.parityShards)
	if err != nil {
		return err
	}

	// Open the inputs
	shards, size, err := t.buildInputShardReaders()
	if err != nil {
		return err
	}

	// Verify the shards
	ok, err := enc.Verify(shards)
	if !ok {
		log.Println("Verification failed. Reconstructing data")
		// Create out destination writers
		out := make([]io.Writer, len(shards))
		for i := range out {
			if shards[i] == nil {
				log.Println("Creating shard", i)
				out[i], err = t.shardCreater(i)
				if err != nil {
					return errors.Wrapf(err, "fail create shard %d", i)
				}
			}
		}
		err = enc.Reconstruct(shards, out)
		if err != nil {
			return errors.Wrap(err, "reconstruct failed")
		}
		// Close output.
		for i := range out {
			if out[i] != nil {
				err := out[i].(io.Closer).Close()
				if err != nil {
					return errors.Wrapf(err, "fail close shard %d", i)
				}
			}
		}
		shards, size, err = t.buildInputShardReaders()
		if err != nil {
			return err
		}
		ok, err = enc.Verify(shards)
		if err != nil {
			return errors.Wrap(err, "Verification failed after reconstruction")
		}
		if !ok {
			return errors.New("Verification failed after reconstruction, data likely corrupted")
		}
	}

	shards, size, err = t.buildInputShardReaders()
	// Join the shards and write them
	// We don't know the exact filesize.
	return enc.Join(t.output, shards, int64(t.dataShards)*size)
}
