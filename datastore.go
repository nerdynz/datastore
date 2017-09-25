package datastore

import (
	"database/sql"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johntdyer/slackrus"
	_ "github.com/mattes/migrate/driver/postgres" //for migrations
	"github.com/mattes/migrate/migrate"
	"github.com/unrolled/render"

	dotenv "github.com/joho/godotenv"
	dat "gopkg.in/mgutz/dat.v1"
	runner "gopkg.in/mgutz/dat.v1/sqlx-runner"
	redis "gopkg.in/redis.v5"
)

type Datastore struct {
	Renderer *render.Render
	DB       *runner.DB
	Cache    *redis.Client
	Settings *Settings
	S3       *s3.S3
}

// New - returns a new datastore which contains redis, database, view globals and settings.
func New() *Datastore {
	store := &Datastore{}
	settings := loadSettings()

	// - LOGGING
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})
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
	store.DB = getDBConnection(settings)
	store.Cache = getCacheConnection(settings)
	store.S3 = getS3Connection()
	return store
}

func getDBConnection(settings *Settings) *runner.DB {
	//get url from ENV in the following format postgres://user:pass@192.168.8.8:5432/spaceio")
	dbURL := os.Getenv("DATABASE_URL")
	log.Info(dbURL)

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
	}
	log.Info("database running")
	// ensures the database can be pinged with an exponential backoff (15 min)
	runner.MustPing(db)

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
	Sitename             string
	EncKey               string
	ServerPort           string
	AttachmentsFolder    string
	Proto                string
	SlackLogURL          string
	CheckCSRFViaReferrer bool
}

func loadSettings() *Settings {
	err := dotenv.Load()
	if err != nil {
		panic(err)
	}
	s := &Settings{}
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
	s.AttachmentsFolder = os.Getenv("ATTACHMENTS_FOLDER")
	s.CanonicalURL = strings.ToLower(os.Getenv("CANONICAL_URL"))
	s.CheckCSRFViaReferrer = s.Sitename != "displayworks" // almost always true for backwards compatibility
	s.SlackLogURL = os.Getenv("SLACK_LOG_URL")
	if s.SlackLogURL == "" {
		s.SlackLogURL = "https://hooks.slack.com/services/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"
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

func getS3Connection() *s3.S3 {
	id := os.Getenv("AWS_ACCESS_KEY_ID")
	if id == "" {
		log.Error("AWS_ACCESS_KEY_ID not specified")
	}
	key := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if key == "" {
		log.Error("AWS_SECRET_ACCESS_KEY not specified")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		log.Error("AWS_REGION not specified")
	}
	token := ""

	creds := credentials.NewStaticCredentials(id, key, token)
	_, err := creds.Get()
	if err != nil {
		log.Error("AWS authentication error")
	}
	cfg := aws.NewConfig().WithRegion(region).WithCredentials(creds)
	s := s3.New(session.New(), cfg)
	return s
}
