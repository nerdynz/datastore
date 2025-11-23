package datastore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/oklog/ulid/v2"
	sqldblogger "github.com/simukti/sqldb-logger"
)

type Publisher interface {
	Publish(siteUlid string, entity string, messageType string, ids []string) error
}

type Datastore struct {
	*pgxpool.Pool
	Publisher   Publisher
	Cache       Cache
	Settings    Settings
	FileStorage FileStorage
}

func ULID() string {
	return ulid.Make().String()
}

type Settings interface {
	Get(key string) string
	GetDuration(key string) time.Duration
	GetBool(key string) bool
	IsProduction() bool
	IsDevelopment() bool
}

type Cache interface {
	Set(ctx context.Context, key string, value string, duration time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	Expire(ctx context.Context, key string) error
	Del(ctx context.Context, key string) error
	GetBytes(ctx context.Context, key string) ([]byte, error)
	SetBytes(ctx context.Context, key string, value []byte, duration time.Duration) error
	FlushDB(ctx context.Context) error
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

type FileStorage interface {
	OpenFile(fileIdentifier string) (b []byte, fileid string, fullURL string, err error)
	GetURL(fileIdentifier string) (fullURL string)
	SaveFile(fileIdentifier string, b io.Reader, sanitizePath bool) (fileid string, fullURL string, err error)
}

type PgxDefaultType struct {
	Value any
	Name  string
}

type DatastoreConfig struct {
	DatabaseURL     string
	PGXTypes        []*pgtype.Type
	PGXDefaultTypes []PgxDefaultType
}

// New - returns a new datastore which contains redis, database and settings.
// everything in the datastore should be concurrent safe and stand within thier own right. i.e. accessible at anypoint from the app
func New(settings Settings, cache Cache, filestorage FileStorage) *Datastore {
	return NewWithConfig(settings, cache, filestorage, DatastoreConfig{
		DatabaseURL: settings.Get("DATABASE_URL"),
	})
}

func NewWithConfig(settings Settings, cache Cache, filestorage FileStorage, conf DatastoreConfig) *Datastore {
	config, err := pgxpool.ParseConfig(conf.DatabaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	config.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger:   &slogPgxAdapter{logger: slog.Default()},
		LogLevel: tracelog.LogLevelTrace,
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		for _, typeMap := range conf.PGXTypes {
			conn.TypeMap().RegisterType(typeMap)
		}
		for _, PgxDefaultType := range conf.PGXDefaultTypes {
			conn.TypeMap().RegisterDefaultPgType(PgxDefaultType.Value, PgxDefaultType.Name)
		}
		return conn.Ping(ctx)
	}
	conn, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	// defer conn.Close(context.Background())
	store := Simple(conn)
	store.Settings = settings

	store.Cache = cache
	store.FileStorage = filestorage

	return store
}

func (ds *Datastore) Tx(ctx context.Context) (context.Context, error) {
	tx, err := ds.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, "tx", tx), nil
}

func (ds *Datastore) Commit(ctx context.Context) error {
	tx, ok := ctx.Value("tx").(pgx.Tx)
	if !ok {
		return errors.New("tx not found")
	}

	err := tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (ds *Datastore) Rollback(ctx context.Context) error {
	tx, ok := ctx.Value("tx").(pgx.Tx)
	if !ok {
		return errors.New("tx not found")
	}

	err := tx.Rollback(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (ds *Datastore) Select(ctx context.Context, records any, sql string, args ...interface{}) error {
	return pgxscan.Select(ctx, ds, records, sql, args...)
}

func (ds *Datastore) One(ctx context.Context, record any, sql string, args ...interface{}) error {
	return pgxscan.Get(ctx, ds, record, sql, args...)
}

func Simple(c *pgxpool.Pool) *Datastore {
	store := &Datastore{
		Pool: c,
	}
	return store
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
