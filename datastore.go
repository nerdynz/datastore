package datastore

import (
	"errors"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/nerdynz/dat/dat"
	runner "github.com/nerdynz/dat/sqlx-runner"
	"github.com/nerdynz/security"
	sqldblogger "github.com/simukti/sqldb-logger"
	"github.com/simukti/sqldb-logger/logadapter/logrusadapter"
	"github.com/sirupsen/logrus"
)

type Publisher interface {
	Publish(siteUlid string, entity string, messageType string, ids []string) error
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
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Fatalln(args ...interface{})
}

type Datastore struct {
	DB          *runner.DB
	Publisher   Publisher
	Cache       Cache
	Settings    Settings
	Key         security.Key
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

func (ds *Datastore) Publish(siteUlid string, entity string, messageType string, ids []string) error {
	return ds.Publisher.Publish(siteUlid, entity, messageType, ids)
}

// func (ds *Datastore) TurnOnLogging() { // delibrately removed in favor of "github.com/simukti/sqldb-logger"
// 	dat.SetDebugLogger(ds.Logger.Warnf)
// 	dat.SetSQLLogger(ds.Logger.Infof)
// 	dat.SetErrorLogger(ds.Logger.Errorf)
// }

func (ds *Datastore) TurnOffLogging() {
	dat.SetDebugLogger(nil)
	dat.SetSQLLogger(nil)
	dat.SetErrorLogger(nil)
}

type FileStorage interface {
	OpenFile(fileIdentifier string) (b []byte, fileid string, fullURL string, err error)
	GetURL(fileIdentifier string) (fullURL string)
	SaveFile(fileIdentifier string, b io.Reader, sanitizePath bool) (fileid string, fullURL string, err error)
}

// New - returns a new datastore which contains redis, database and settings.
// everything in the datastore should be concurrent safe and stand within thier own right. i.e. accessible at anypoint from the app
func New(logger Logger, settings Settings, cache Cache, filestorage FileStorage) *Datastore {
	store := Simple()
	store.Logger = logger
	store.Settings = settings
	store.DB = getDBConnection(store, cache)
	store.Cache = cache
	store.FileStorage = filestorage

	store.TurnOffLogging()
	return store
}

func (ds *Datastore) SetKey(key security.Key) {
	ds.Key = key
}

func (ds *Datastore) Cleanup() {
	ds.Logger.Info("Cleanup")
	ds.DB.DB.Close()
	// ds.Cache.Client.Close()
}

func Simple() *Datastore {
	store := &Datastore{}
	return store
}

func getDBConnection(store *Datastore, cache Cache) *runner.DB {
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

	dsn := dbStr + " password=" + pass + " sslmode=disable "
	db := sqldblogger.OpenDriver(dsn, &pq.Driver{}, logrusadapter.New(logrus.StandardLogger()),
		// AVAILABLE OPTIONS
		// sqldblogger.WithErrorFieldname("sql_error"),                   // default: error
		// sqldblogger.WithDurationFieldname("query_duration"),           // default: duration
		// sqldblogger.WithTimeFieldname("time"), // default: time
		// sqldblogger.WithSQLQueryFieldname("sql_query"),                // default: query
		// sqldblogger.WithSQLArgsFieldname("sql_args"),                  // default: args
		// sqldblogger.WithMinimumLevel(sqldblogger.LevelDebug),          // default: LevelDebug
		sqldblogger.WithLogArguments(true),                            // default: true
		sqldblogger.WithDurationUnit(sqldblogger.DurationMillisecond), // default: DurationMillisecond
		sqldblogger.WithTimeFormat(sqldblogger.TimeFormatRFC3339),     // default: TimeFormatUnix
		sqldblogger.WithLogDriverErrorSkip(false),                     // default: false
		sqldblogger.WithSQLQueryAsMessage(true),                       // default: false
		// sqldblogger.WithUIDGenerator(sqldblogger.UIDGenerator),       // default: *defaultUID
		// sqldblogger.WithConnectionIDFieldname("con_id"),  // default: conn_id
		// sqldblogger.WithStatementIDFieldname("stm_id"),   // default: stmt_id
		// sqldblogger.WithTransactionIDFieldname("trx_id"), // default: tx_id
		sqldblogger.WithWrapResult(true),        // default: true
		sqldblogger.WithIncludeStartTime(false), // default: false
		// sqldblogger.WithStartTimeFieldname("start_time"), // default: start
		// sqldblogger.WithPreparerLevel(sqldblogger.LevelDebug), // default: LevelInfo
		// sqldblogger.WithQueryerLevel(sqldblogger.LevelDebug),  // default: LevelInfo
		// sqldblogger.WithExecerLevel(sqldblogger.LevelDebug),   // default: LevelInfo
	)
	err = db.Ping()
	if err != nil {
		store.Logger.Fatal("postgres failed to connect %s", err.Error())
	}
	store.Logger.Info("database running")
	// ensures the database can be pinged with an exponential backoff (15 min)
	runner.MustPing(db)

	// rawDb := sqldblogger.OpenDriver("file:tst2.db?cache=shared&mode=rwc&_journal_mode=WAL", &sqlite3.SQLiteDriver{}, logrusadapter.New(logrus.StandardLogger()))

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
