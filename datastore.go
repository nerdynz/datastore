package datastore

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	// "github.com/jackc/pgx/v4/pgxpool"
	_ "gopkg.in/mattes/migrate.v1/driver/postgres" //for migrations
)

// Logger - designed as a drop in for logrus with some other backwards compat stuff
type Logger interface {
	SetOutput(out io.Writer)
	Print(args ...interface{})
	Printf(format string, args ...interface{})
	Println(args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Infoln(args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Warnln(args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Errorln(args ...interface{})
}

type Datastore struct {
	DB          *pgxpool.Pool
	Cache       Cache
	Settings    Settings
	Logger      Logger
	FileStorage FileStorage
}

type Settings interface {
	Get(key string) string
	GetDuration(key string) time.Duration
	GetBool(key string) bool
	IsProduction() bool
	IsDevelopment() bool
}

type Cache interface {
	Set(key string, value string, duration time.Duration) error
	Get(key string) (string, error)
	Expire(key string) error
	Del(key string) error
	GetBytes(key string) ([]byte, error)
	SetBytes(key string, value []byte, duration time.Duration) error
	FlushDB() error
	// Close() error
}

func (ds *Datastore) TurnOnLogging() {
	// dat.SetDebugLogger(ds.Logger.Warnf)
	// dat.SetSQLLogger(ds.Logger.Infof)
	// dat.SetErrorLogger(ds.Logger.Errorf)
}

func (ds *Datastore) TurnOffLogging() {
	// dat.SetDebugLogger(nil)
	// dat.SetSQLLogger(nil)
	// dat.SetErrorLogger(nil)
}

type FileStorage interface {
	OpenFile(fileIdentifier string) (b []byte, fileid string, fullURL string, err error)
	GetURL(fileIdentifier string) (fullURL string)
	SaveFile(fileIdentifier string, b io.Reader, sanitizePath bool) (fileid string, fullURL string, err error)
}

// New - returns a new datastore which contains redis, database and settings.
// everything in the datastore should be concurrent safe and stand within thier own right. i.e. accessible at anypoint from the app
func New(logger Logger, settings Settings, cache Cache, filestorage FileStorage) *Datastore {
	store := &Datastore{}
	store.Logger = logger
	store.Settings = settings
	store.Cache = cache
	store.FileStorage = filestorage

	// conf, err := pgx.ParseConfig()
	// if err != nil {
	// 	store.Logger.Error(err)
	// 	os.Exit(1)
	// }
	// db, _ := pgxpool.Connect(ctx, "example-connection-url")``
	conn, err := pgxpool.Connect(context.Background(), store.Settings.Get("DATABASE_URL"))
	if err != nil {
		store.Logger.Error(err)
		os.Exit(1)
	}

	store.DB = conn
	return store
}

func (ds *Datastore) Cleanup() {
	ds.DB.Close()
	// ds.Cache.Close()
}
