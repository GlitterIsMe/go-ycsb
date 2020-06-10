module github.com/GlitterIsMe/go-ycsb

require (
	cloud.google.com/go/spanner v1.1.0
	github.com/XiaoMi/pegasus-go-client v0.0.0-20181029071519-9400942c5d1c
	github.com/aerospike/aerospike-client-go v1.35.2
	github.com/apple/foundationdb/bindings/go v0.0.0-20200112054404-407dc0907f4f
	github.com/boltdb/bolt v1.3.1
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/dgraph-io/badger v1.5.4
	github.com/go-redis/redis v6.15.1+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocql/gocql v0.0.0-20181124151448-70385f88b28b
	github.com/google/btree v1.0.0 // indirect
	github.com/lib/pq v0.0.0-20181016162627-9eb73efc1fcc
	github.com/magiconair/properties v1.8.0
	github.com/mattn/go-sqlite3 v1.10.0
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/pingcap/errors v0.11.1
	github.com/pingcap/go-ycsb v0.0.0-20200226103513-00ca633a87d8
	github.com/spf13/cobra v0.0.3
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	github.com/tikv/client-go v0.0.0-20190421092910-44b82dcc9f4a
	go.mongodb.org/mongo-driver v1.0.2
	google.golang.org/api v0.14.0
	google.golang.org/genproto v0.0.0-20191206224255-0243a4be9c8f
)

replace github.com/apache/thrift => github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d

go 1.13
