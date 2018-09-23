package datastore

import (
	"database/sql"
	"encoding/json"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/johntdyer/slackrus"
	log "github.com/sirupsen/logrus"
	"github.com/unrolled/render"
	_ "gopkg.in/mattes/migrate.v1/driver/postgres" //for migrations
	"gopkg.in/mattes/migrate.v1/migrate"

	dotenv "github.com/joho/godotenv"
	dat "github.com/nerdynz/dat"
	"github.com/nerdynz/dat/kvs"
	runner "github.com/nerdynz/dat/sqlx-runner"
	redis "gopkg.in/redis.v5"
)

type Datastore struct {
	Renderer  *render.Render
	DB        *runner.DB
	Cache     *redis.Client
	Settings  *Settings
	Websocket Websocket
}

type Logger struct {
	errLog string
}

func NewLogger() *Logger {
	return &Logger{}
}

func (l *Logger) Write(b []byte) (int, error) {
	l.errLog += string(b) + "\n"
	return len(b), nil
}

func (l *Logger) LogBytes(b []byte) {
	l.errLog += string(b) + "\n"
}

func (l *Logger) LogText(text string) {
	l.errLog += text
}

func (l *Logger) LogValue(val interface{}) {
	b, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		l.LogText("failed to marshal val")
	}
	l.LogBytes(b)
}

func (l *Logger) ResetLog() {
	l.errLog = "" //reset it
}

func (l *Logger) PrintLog() {
	log.Info(l.errLog)
}

func (l *Logger) GetLog() string {
	return l.errLog
}

// New - returns a new datastore which contains redis, database, view globals and settings.
func New() *Datastore {
	store := Simple()
	store.Cache = getCacheConnection(store.Settings)
	store.DB = getDBConnection(store.Settings)
	return store
}

func (ds Datastore) Cleanup() {
	ds.DB.DB.Close()
	ds.Cache.Close()
}

func Simple() *Datastore {
	store := &Datastore{}
	settings := loadSettings()

	// - LOGGING
	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&log.JSONFormatter{})
	// Output to stderr instead of stdout, could also be a file.
	log.SetOutput(os.Stderr)
	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)

	channel := "#logs-" + settings.Sitename
	emoji := ":dog:"
	acceptedLevels := slackrus.LevelThreshold(log.InfoLevel)
	if settings.ServerIsDEV {
		emoji = ":hamster:"
		channel = "#logs" // dont care about the sitename
		acceptedLevels = slackrus.LevelThreshold(log.ErrorLevel)
	}

	logHook := &slackrus.SlackrusHook{
		HookURL:        settings.SlackLogURL,
		AcceptedLevels: acceptedLevels,
		Channel:        channel,
		IconEmoji:      emoji,
		Username:       settings.Sitename,
	}
	log.AddHook(logHook)
	log.Info("App Started. Server Is: " + settings.ServerIs)

	store.Settings = settings

	// store.S3 = getS3Connection()
	return store
}

func getDBConnection(settings *Settings) *runner.DB {
	//get url from ENV in the following format postgres://user:pass@192.168.8.8:5432/spaceio")
	dbURL := os.Getenv("DATABASE_URL")
	u, err := url.Parse(dbURL)
	if err != nil {
		log.Error(err)
	}

	username := u.User.Username()
	pass, isPassSet := u.User.Password()
	if !isPassSet {
		log.Error("no database password")
	}
	host, port, _ := net.SplitHostPort(u.Host)
	dbName := strings.Replace(u.Path, "/", "", 1)

	db, _ := sql.Open("postgres", "dbname="+dbName+" user="+username+" password="+pass+" host="+host+" port="+port+" sslmode=disable")
	err = db.Ping()
	if err != nil {
		log.Error(err)
		panic(err)
	}
	log.Info("database running")
	// ensures the database can be pinged with an exponential backoff (15 min)
	runner.MustPing(db)

	if settings.CacheNamespace != "" {
		store, err := kvs.NewRedisStore(settings.CacheNamespace, ":6379", "")
		if err != nil {
			log.Error(err)
			panic(err)
		}
		log.Info("USING CACHE", settings.CacheNamespace)
		runner.SetCache(store)
	}

	// set to reasonable values for production
	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(16)

	// set this to enable interpolation
	dat.EnableInterpolation = true

	// set to check things like sessions closing.
	// Should be disabled in production/release builds.
	dat.Strict = false

	// Log any query over 10ms as warnings. (optional)
	if settings.ServerIsDEV {
		runner.LogQueriesThreshold = 1 * time.Microsecond
	}

	if settings.ServerIsLVE {
		log.Info("migrating")
		errs, ok := migrate.UpSync(settings.DSN+"?sslmode=disable", "./server/models/migrations")
		if !ok {
			finalError := ""
			for _, err := range errs {
				finalError += err.Error() + "\n"
			}
			log.Error(finalError)
		}
	}

	// db connection
	return runner.NewDB(db, "postgres")
}

func getCacheConnection(settings *Settings) *redis.Client {

	opts := &redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}

	url := os.Getenv("REDIS_URL")
	if url != "" {
		newOpts, err := redis.ParseURL(url)
		if err == nil {
			opts = newOpts
		} else {
			log.Error(err)
			return nil
		}
	}

	client := redis.NewClient(opts)
	pong, err := client.Ping().Result()
	if err != nil {
		log.Error(err)
		return nil
	}

	log.Info("cache running", pong)
	return client
}

// Settings - common settings used around the site. Currently loaded into the datastore object
type Settings struct {
	ServerIsDEV          bool
	ServerIsLVE          bool
	ServerIs             string
	DSN                  string
	CanonicalURL         string
	WebsiteBaseURL       string
	Sitename             string
	EncKey               string
	ServerPort           string
	AttachmentsFolder    string
	MaxImageWidth        int
	IsSecured            bool
	Proto                string
	SlackLogURL          string
	CheckCSRFViaReferrer bool
	EmailFromName        string
	EmailFromEmail       string
	IsSiteBound          bool
	CacheNamespace       string
	LoggingEnabled       bool
}

func loadSettings() *Settings {
	err := dotenv.Load()
	if err != nil {
		panic(err)
	}
	s := &Settings{}
	s.LoggingEnabled = (os.Getenv("LOGGING_ENABLED") == "true")
	s.ServerIsDEV = (os.Getenv("IS_DEV") == "true")
	s.ServerIsLVE = !s.ServerIsDEV
	if s.ServerIsDEV {
		s.ServerIs = "DEV"
	}
	if s.ServerIsLVE {
		s.ServerIs = "LVE"
	}
	s.DSN = os.Getenv("DATABASE_URL")
	s.Sitename = os.Getenv("SITE_NAME")
	s.EncKey = os.Getenv("SECURITY_ENCRYPTION_KEY")
	s.CacheNamespace = os.Getenv("CACHE_NAMESPACE")

	s.EmailFromName = os.Getenv("EMAIL_FROM_NAME")
	if s.EmailFromName == "" {
		s.EmailFromName = "Josh Developer"
	}
	s.EmailFromEmail = os.Getenv("EMAIL_FROM_EMAIL")
	if s.EmailFromEmail == "" {
		s.EmailFromEmail = "josh@nerdy.co.nz"
	}

	s.MaxImageWidth = 1920
	imgWidth := os.Getenv("MAX_IMAGE_WIDTH")
	if imgWidth != "" {
		newWidth, err := strconv.Atoi(imgWidth)
		if err == nil {
			s.MaxImageWidth = newWidth
		}
	}
	s.AttachmentsFolder = os.Getenv("ATTACHMENTS_FOLDER")
	s.CanonicalURL = strings.ToLower(os.Getenv("CANONICAL_URL"))
	s.CheckCSRFViaReferrer = s.Sitename != "displayworks" // almost always true for backwards compatibility
	s.SlackLogURL = os.Getenv("SLACK_LOG_URL")
	s.IsSecured = (os.Getenv("IS_HTTPS") == "true")
	s.Proto = "http://"
	if s.IsSecured {
		s.Proto = "https://"
	}
	s.IsSiteBound = strings.ToLower(os.Getenv("IS_SITE_BOUND")) == "true"
	s.WebsiteBaseURL = os.Getenv("WEBSITE_BASE_URL")
	if s.WebsiteBaseURL == "" {
		s.WebsiteBaseURL = s.Proto + s.CanonicalURL + "/"
	}
	if len(os.Getenv("DISABLE_CSRF")) > 0 { // for backwards compatibility
		s.CheckCSRFViaReferrer = false
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = ":80"
	} else {
		port = ":" + port // append the :
	}
	s.ServerPort = port
	return s
}

func (s *Settings) Get(setting string) string {
	return os.Getenv(setting)
}

// func getS3Connection() *s3.S3 {
// 	id := os.Getenv("AWS_ACCESS_KEY_ID")
// 	if id == "" {
// 		log.Error("AWS_ACCESS_KEY_ID not specified")
// 	}
// 	key := os.Getenv("AWS_SECRET_ACCESS_KEY")
// 	if key == "" {
// 		log.Error("AWS_SECRET_ACCESS_KEY not specified")
// 	}
// 	region := os.Getenv("AWS_REGION")
// 	if region == "" {
// 		log.Error("AWS_REGION not specified")
// 	}
// 	token := ""

// 	creds := credentials.NewStaticCredentials(id, key, token)
// 	_, err := creds.Get()
// 	if err != nil {
// 		log.Error("AWS authentication error")
// 	}
// 	cfg := aws.NewConfig().WithRegion(region).WithCredentials(creds)
// 	s := s3.New(session.New(), cfg)
// 	return s
// }

type Websocket interface {
	Broadcast(string, string) error
}

func (ds *Datastore) GetCacheValue(key string) (string, error) {
	val := ds.Cache.Get(key)
	return val.Result()
}
func (ds *Datastore) GetCacheBytes(key string) ([]byte, error) {
	val := ds.Cache.Get(key)
	return val.Bytes()
}

func (ds *Datastore) SetCacheValue(key string, value interface{}, duration time.Duration) (string, error) {
	val := ds.Cache.Set(key, value, duration)
	return val.Result()
}
