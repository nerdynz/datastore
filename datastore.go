package datastore

import (
	"database/sql"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	_ "gopkg.in/mattes/migrate.v1/driver/postgres" //for migrations
	"gopkg.in/mattes/migrate.v1/migrate"

	dat "github.com/nerdynz/dat/dat"
	runner "github.com/nerdynz/dat/sqlx-runner"
)

type Websocket interface {
	Broadcast(string, string) error
}

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
	DB          *runner.DB
	Cache       Cache
	Settings    Settings
	Websocket   Websocket
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
}

func (ds *Datastore) TurnOnLogging() {
	dat.SetDebugLogger(ds.Logger.Warnf)
	dat.SetSQLLogger(ds.Logger.Infof)
	dat.SetErrorLogger(ds.Logger.Errorf)
}

func (ds *Datastore) TurnOffLogging() {
	dat.SetDebugLogger(nil)
	dat.SetSQLLogger(nil)
	dat.SetErrorLogger(nil)
}

type FileStorage interface {
	OpenFile(fileIdentifier string) (b []byte, fileid string, fullURL string, err error)
	GetURL(fileIdentifier string) (fullURL string)
	// always returning bytes might be a little expensive, but it makes the interface much more reasonable
	SaveFile(fileIdentifier string, b io.Reader) (fileid string, fullURL string, err error)
}

// type Logger struct {
// 	errLog string
// }

// func NewLogger() *Logger {
// 	return &Logger{}
// }

// func (l *Logger) Write(b []byte) (int, error) {
// 	l.errLog += string(b) + "\n"
// 	return len(b), nil
// }

// func (l *Logger) LogBytes(b []byte) {
// 	l.errLog += string(b) + "\n"
// }

// func (l *Logger) LogText(text string) {
// 	l.errLog += text
// }

// func (l *Logger) LogValue(val interface{}) {
// 	b, err := json.MarshalIndent(val, "", "  ")
// 	if err != nil {
// 		l.LogText("failed to marshal val")
// 	}
// 	l.LogBytes(b)
// }

// func (l *Logger) ResetLog() {
// 	l.errLog = "" //reset it
// }

// func (l *Logger) PrintLog() {
// 	log.Info(l.errLog)
// }

// func (l *Logger) GetLog() string {
// 	return l.errLog
// }

// New - returns a new datastore which contains redis, database and settings.
// everything in the datastore should be concurrent safe and stand within thier own right. i.e. accessible at anypoint from the app
func New(logger Logger, settings Settings, cache Cache, filestorage FileStorage, ws Websocket) *Datastore {
	store := Simple()
	store.Logger = logger
	store.Settings = settings
	store.DB = getDBConnection(store, cache)
	store.Cache = cache
	store.FileStorage = filestorage
	return store
}

func (ds *Datastore) Cleanup() {
	ds.DB.DB.Close()
	// ds.Cache.Client.Close()
}

func Simple() *Datastore {
	store := &Datastore{}
	return store
}

func getDBConnection(store *Datastore, cache Cache) *runner.DB {
	//get url from ENV in the following format postgres://user:pass@192.168.8.8:5432/spaceio")
	dbURL := store.Settings.Get("DATABASE_URL")
	u, err := url.Parse(dbURL)
	if err != nil {
		store.Logger.Error(err)
	}

	username := u.User.Username()
	pass, isPassSet := u.User.Password()
	if !isPassSet {
		store.Logger.Error("no database password")
	}
	host, port, _ := net.SplitHostPort(u.Host)
	dbName := strings.Replace(u.Path, "/", "", 1)

	if host == "GCLOUD_SQL_INSTANCE" {
		// USE THE GCLOUD_SQL_INSTANCE SETTING instead... e.g. host= /cloudsql/INSTANCE_CONNECTION_NAME
		host = store.Settings.Get("GCLOUD_SQL_INSTANCE")
	}

	dbStr := "dbname=" + dbName + " user=" + username + " host=" + host
	if port != "" {
		dbStr += " port=" + port
	}
	store.Logger.Info(dbStr)

	db, _ := sql.Open("postgres", dbStr+" password="+pass+" sslmode=disable ") // pass goes last
	err = db.Ping()
	if err != nil {
		store.Logger.Error(err)
		panic(err)
	}
	store.Logger.Info("database running")
	// ensures the database can be pinged with an exponential backoff (15 min)
	runner.MustPing(db)

	// if store.Settings.Get("CACHE_NAMESPACE") != "" {
	// 	redisUrl := ":6379"
	// 	if store.Settings.Get("CACHE_URL") != "" {
	// 		redisUrl = store.Settings.Get("CACHE_URL")
	// 	}
	// 	cache, err := kvs.NewRedisStore(store.Settings.Get("CACHE_NAMESPACE"), redisUrl, "")
	// 	if err != nil {
	// 		store.Logger.Error(err)
	// 		panic(err)
	// 	}
	// 	store.Logger.Info("USING CACHE", store.Settings.Get("CACHE_NAMESPACE"))
	// 	runner.SetCache(cache)
	// }
	if cache != nil {
		runner.SetCache(cache)
	}

	// set to reasonable values for production
	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(16)

	// set this to enable interpolation
	dat.EnableInterpolation = true

	if store.Settings.IsProduction() {
		// PRODUCTION
		errs, ok := migrate.UpSync(dbURL+"?sslmode=disable", "./server/models/migrations")
		if !ok {
			finalError := ""
			for _, err := range errs {
				finalError += err.Error() + "\n"
			}
			store.Logger.Error(finalError)
		}
	} else {
		// DEV`
		// set to check things like sessions closing.
		// Should be disabled in production/release builds.
		dat.Strict = true

		// Log any query over 10ms as warnings. (optional)
		// runner.LogQueriesThreshold = 1 * time.Microsecond // LOG EVERYTHING ON DEV // turn it off and on with a flag
	}

	// db connection
	return runner.NewDB(db, "postgres")
}
