// +build leveldb

package leveldb

import (
	"github.com/GlitterIsMe/go-ycsb/pkg/prop"
	"github.com/GlitterIsMe/go-ycsb/pkg/util"
	"github.com/GlitterIsMe/go-ycsb/pkg/ycsb"
	"github.com/GlitterIsMe/goleveldb_wrapper"
	"github.com/magiconair/properties"
)

const (
	leveldbDir = "leveldb.dir"
	// Options
	leveldbCreateIfMissing      = "leveldb.create_if_missing"
	leveldbErrorIfExists        = "leveldb.error_if_exists"
	leveldbParanoidChecks       = "leveldb.paranoid_checks"
	leveldbWriteBufferSize      = "leveldb.write_buffer_size"
	leveldbMaxOpenFiles         = "leveldb.max_open_files"
	leveldbBlockSize            = "leveldb.block_size"
	leveldbBlockRestartInterval = "leveldb.block_restart_interval"
	leveldbCompression          = "leveldb.compression"

	// ReadOptions
	leveldbVerifyChecksums = "leveldb.verify_checksums"
	leveldbFillCache       = "leveldb.fill_cache"

	// WriteOptions
	leveldbSync = "leveldb.sync"
)

type levelDBCreator struct{}

type levelDB struct {
	p  *properties.Properties
	db *goleveldb_wrapper.DB

	r       *util.RowCodec
	bufPool *util.BufPool

	readOpt  *goleveldb_wrapper.ReadOptions
	writeOpt *goleveldb_wrapper.WriteOptions
}

func (c levelDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dir := p.GetString(levelDir, "/tmp/leveldb")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(dir)
	}

	opts := getOptions(p)

	db, err := goleveldb_wrapper.OpenDB(opts, dir)
	if err != nil {
		return nil, err
	}

	return &rocksDB{
		p:         p,
		db:        db,
		r:         util.NewRowCodec(p),
		bufPool:   util.NewBufPool(),
		readOpts:  goleveldb_wrapper.NewReadOptions(),
		writeOpts: goleveldb_wrapper.NewWriteOptions(),
	}, nil
}

func getOptions(p *properties.Properties) *goleveldb_wrapper.Options {
	opts := goleveldb_wrapper.NewOptions()

	opt.SetComparator(goleveldb_wrapper.NewComparator())
	opt.SetCreateIfMissing(p.GetBool(leveldbCreateIfMissing, true))
	opt.SetErrorIfExists(p.GetBool(leveldbErrorIfExists, true))
	opt.SetParanoidChecks(p.GetBool(leveldbParanoidChecks, false))
	opt.SetEnv(goleveldb_wrapper.NewDefaultEnv())
	opt.SetWriteBufferSize(p.GetUint64(leveldbWriteBufferSize, 4<<20))
	opt.SetMaxOpenFiles(p.GetInt(leveldbMaxOpenFiles, 1000))
	opt.SetCache(goleveldb_wrapper.NewCache(16 * 1024 * 1024))
	opt.SetBlockSize(p.GetInt(leveldbBlockSize, 4096))
	opt.SetBlockRestartInterval(p.Getint(leveldbBlockRestartInterval, 16))
	opt.SetCompression(p.GetInt(leveldbCompression, goleveldb_wrapper.SnappyCompression))
}

func (db *levelDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *levelDB) close() error {
	db.db.Close()
	return nil
}

func (db *levelDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *levelDB) CleanupThread(_ context.Context) {
}

func (db *levelDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.db.Get(db.readOpts, db.getRowKey(table, key))
	if err != nil {
		return nil, err
	}

	return db.r.Decode(value, fields)
}

func (db *levelDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	it := db.db.NewIterator(db.readOpts)
	defer it.Close()

	rowStartKey := db.getRowKey(table, startKey)

	it.Seek(rowStartKey)
	i := 0
	for it = it; it.Valid() && i < count; it.Next() {
		value := it.Value()
		m, err := db.r.Decode(value, fields)
		if err != nil {
			return nil, err
		}
		res[i] = m
		i++
	}

	if err := it.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *levelDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), m)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)

	return db.db.Put(db.writeOpts, rowKey, rowData)
}

func (db *levelDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), values)
	if err != nil {
		return err
	}
	return db.db.Put(db.writeOpts, rowKey, rowData)
}

func (db *levelDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)

	return db.db.Delete(db.writeOpts, rowKey)
}

func init() {
	ycsb.RegisterDBCreator("leveldb", levelDBCreator{})
}
