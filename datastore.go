package datastore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/lib/pq"
	"github.com/nerdynz/dat/dat"
	runner "github.com/nerdynz/dat/sqlx-runner"
	"github.com/nerdynz/security"
	sqldblogger "github.com/simukti/sqldb-logger"
)

type Publisher interface {
	Publish(siteUlid string, entity string, messageType string, ids []string) error
}
type Datastore struct {
	*pgxpool.Pool
	DB          *runner.DB
	Publisher   Publisher
	Cache       Cache
	Settings    Settings
	Key         security.Key
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

type slogAdapter struct {
	logger *slog.Logger
}

type slogPgxAdapter struct {
	logger *slog.Logger
}

func (sa *slogAdapter) Log(ctx context.Context, level sqldblogger.Level, msg string, data map[string]any) {
	ll := levelTrace
	switch level {
	case sqldblogger.LevelError:
		ll = levelError
		break
	case sqldblogger.LevelInfo:
		ll = levelInfo
		break
	case sqldblogger.LevelDebug:
		ll = levelDebug
		break
	}
	log(sa.logger, ctx, ll, msg, data)
}

func (sa *slogPgxAdapter) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	ll := levelTrace
	switch level {
	case tracelog.LogLevelError:
		ll = levelError
		break
	case tracelog.LogLevelInfo:
		ll = levelInfo
		break
	case tracelog.LogLevel(levelDebug):
		ll = levelDebug
		break
	}
	log(sa.logger, ctx, ll, msg, data)
}

type loglevel uint8

const (
	// LevelTrace is the lowest level and the most detailed.
	// Use this if you want to know interaction flow from prepare, statement, execution to result/rows.
	levelTrace loglevel = iota
	// LevelDebug is used by non Queryer(Context) and Execer(Context) call like Ping() and Connect().
	levelDebug
	// LevelInfo is used by Queryer, Execer, Preparer, and Stmt.
	levelInfo
	// LevelError is used on actual driver error or when driver not implement some optional sql/driver interface.
	levelError
)

func log(logger *slog.Logger, ctx context.Context, level loglevel, msg string, data map[string]any) {
	args := make([]any, 0, len(data))

	for k, v := range data {
		if k == "time" {
			continue
		}
		k := k
		v := v
		if k == "sql" {
			sql := strings.ReplaceAll(v.(string), "\n", " ")
			sql = strings.ReplaceAll(sql, "\t", " ")
			v = sql
		}
		// if k == "conn_id" {
		// 	continue
		// }

		args = append(args, slog.Any(k, v))
	}
	args = append(args, "sql")
	args = append(args, msg)

	switch level { //nolint:exhaustive
	case levelError:
		logger.ErrorContext(ctx, "DB", args...)
	case levelInfo:
		logger.InfoContext(ctx, "DB", args...)
	case levelDebug:
		logger.DebugContext(ctx, "DB", args...)
	default:
		logger.DebugContext(ctx, "DB", args...)
	}
}

// func (ds *Datastore) TurnOnLogging() { // delibrately removed in favor of "github.com/simukti/sqldb-logger"
// 	dat.SetDebugLogger(slog.Warnf)
// 	dat.SetSQLLogger(slog.Infof)
// 	dat.SetErrorLogger(slog.Errorf)
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
func New(settings Settings, cache Cache, filestorage FileStorage) *Datastore {
	config, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	config.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger:   &slogPgxAdapter{logger: slog.Default()},
		LogLevel: tracelog.LogLevelTrace,
	}
	conn, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	// defer conn.Close(context.Background())
	store := Simple(conn)
	store.Settings = settings
	store.DB = getDBConnection(store, cache)

	store.Cache = cache
	store.FileStorage = filestorage

	store.TurnOffLogging()
	return store
}

func (ds *Datastore) Select(ctx context.Context, records any, sql string, args ...interface{}) error {
	return pgxscan.Select(ctx, ds, records, sql, args...)
}

func (ds *Datastore) One(ctx context.Context, record any, sql string, args ...interface{}) error {
	return pgxscan.Get(ctx, ds, record, sql, args...)
}

func (ds *Datastore) SetKey(key security.Key) {
	ds.Key = key
}

func (ds *Datastore) Cleanup() {
	slog.Info("Cleanup")
	ds.DB.DB.Close()
	// ds.Cache.Client.Close()
}

func Simple(c *pgxpool.Pool) *Datastore {
	store := &Datastore{
		Pool: c,
	}
	return store
}

func getDBConnection(store *Datastore, cache Cache) *runner.DB {
	dbURL := store.Settings.Get("DATABASE_URL")
	u, err := url.Parse(dbURL)
	if err != nil {
		slog.Error("Fatal database parse error", "error", err)
		os.Exit(0)
	}

	username := u.User.Username()
	pass, isPassSet := u.User.Password()
	if !isPassSet {
		slog.Error("no database password")
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
	slog.Info("Connecting to ", "db", dbName, "user", username, "host", host)

	dsn := dbStr + " password=" + pass + " sslmode=disable "
	db := sqldblogger.OpenDriver(dsn, &pq.Driver{}, &slogAdapter{
		slog.Default(),
	},
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

	slog.Info("database running")
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
	// 		slog.Error(err)
	// 		panic(err)
	// 	}
	// 	slog.Info("USING CACHE", store.Settings.Get("CACHE_NAMESPACE"))
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

	if store.Settings.IsDevelopment() {
		// DEV`
		// set to check things like sessions closing.
		// Should be disabled in production/release builds.
		dat.Strict = true
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
	if strings.Contains(whereSQLOrMap, " site_ulid = $") {
		return whereSQLOrMap, args, nil
	}
	if !strings.Contains(whereSQLOrMap, "$SITEULID") {
		return whereSQLOrMap, args, errors.New("No $SITEULID placeholder defined in " + whereSQLOrMap)
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
