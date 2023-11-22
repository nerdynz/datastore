package datastore

import (
	"database/sql"
	"errors"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	_ "gopkg.in/mattes/migrate.v1/driver/postgres" //for migrations
	"gopkg.in/mattes/migrate.v1/migrate"
	"gopkg.in/olahol/melody.v1"

	nats "github.com/nats-io/nats.go"
	dat "github.com/nerdynz/dat/dat"
	runner "github.com/nerdynz/dat/sqlx-runner"
	"github.com/sirupsen/logrus"
)

// type Websocket interface {
// 	Broadcast([]byte) error
// }

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
	WS          *melody.Melody
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
	SaveFile(fileIdentifier string, b io.Reader, sanitizePath bool) (fileid string, fullURL string, err error)
}

// New - returns a new datastore which contains redis, database and settings.
// everything in the datastore should be concurrent safe and stand within thier own right. i.e. accessible at anypoint from the app
func New(logger Logger, settings Settings, cache Cache, filestorage FileStorage) *Datastore {
	store := Simple()
	store.Logger = logger
	store.Settings = settings
	store.DB = getDBConnection(store, cache)
	store.WS = melody.New()
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

func getNatsConnection(store *Datastore, cache Cache) (*nats.Conn, error) {
	return nats.Connect(nats.DefaultURL)
}

func FormatSearch(searchText string) string {
	originalSearchText := searchText
	if searchText != "" {
		var err error
		searchText, err = url.QueryUnescape(searchText)
		if err != nil {
			searchText = originalSearchText
			searchText = strings.Replace(searchText, "%20", "|", -1)
			searchText = strings.Replace(searchText, "", "|", -1)
		}
		searchText = strings.Replace(searchText, "@", " ", -1)
		searchText = strings.Replace(searchText, ".", " ", -1)
		searchText = strings.Trim(searchText, " ")
		searchText = strings.Join(strings.Split(searchText, " "), ":* & ")
		searchText += ":*"
	}
	logrus.Info("searhc", searchText)
	return searchText
}

func AppendSiteULID(siteULID string, whereSQLOrMap string, args ...interface{}) (string, []interface{}, error) {
	if !strings.Contains(whereSQLOrMap, "$SITEULID") {
		return whereSQLOrMap, args, errors.New("No $SITEULID placeholder defined")
	}
	args = append(args, siteULID)
	position := len(args)
	if strings.Contains(whereSQLOrMap, ".$SITEULID") {
		newSQL := strings.Split(whereSQLOrMap, "$SITEULID")[0]
		replaceSQLParts := strings.Split(newSQL, " ")
		replaceSQLTablePrefix := replaceSQLParts[len(replaceSQLParts)-1]

		whereSQLOrMap = strings.Replace(whereSQLOrMap, replaceSQLTablePrefix+"$SITEULID", " and "+replaceSQLTablePrefix+"site_ulid = $"+strconv.Itoa(position), -1)
	} else if strings.Contains(whereSQLOrMap, "$SITEULID") {
		whereSQLOrMap = strings.Replace(whereSQLOrMap, "$SITEULID", " site_ulid = $"+strconv.Itoa(position), -1)
	} else {
		whereSQLOrMap += " and site_ulid = $" + strconv.Itoa(position)
	}
	return whereSQLOrMap, args, nil
}

type PagedData struct {
	Sort      string      `json:"sort"`
	Search    string      `json:"search"`
	Direction string      `json:"direction"`
	Records   interface{} `json:"records"`
	Total     int         `json:"total"`
	PageNum   int         `json:"pageNum"`
	Limit     int         `json:"limit"`
}

func NewPagedData(records interface{}, orderBy string, direction string, search string, itemsPerPage int, pageNum int, total int) *PagedData {
	return &PagedData{
		Records:   records,
		Direction: direction,
		// Sort:      casee.ToPascalCase(orderBy),
		Sort:    orderBy,
		Limit:   itemsPerPage,
		PageNum: pageNum,
		Total:   total,
		Search:  search,
	}
}
