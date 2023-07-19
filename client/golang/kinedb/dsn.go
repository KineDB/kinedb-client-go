package kinedb

import (
	"crypto/rsa"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	errInvalidDSNUnescaped       = errors.New("invalid DSN: did you forget to escape a param value")
	errInvalidDSNAddr            = errors.New("invalid DSN: network address not terminated (missing closing brace)")
	errInvalidDSNNoSlash         = errors.New("invalid DSN: missing the slash separating the database name")
	errInvalidDSNUnsafeCollation = errors.New("invalid DSN: interpolateParams can not be used with unsafe collations")
)

// DsnConfig is a configuration parsed from a DSN string.
// If a new DsnConfig is created instead of being parsed from a DSN string,
// the NewDsnConfig function should be used, which sets default values.
type DsnConfig struct {
	User              string // Username
	Passwd            string // Password (requires User)
	Engine            string
	FetchSize         int32
	enableStreamQuery bool
	Net               string            // Network type
	Addr              string            // Network address (requires Net)
	DBName            string            // Database name
	Params            map[string]string // Connection parameters
	Collation         string            // Connection collation
	Loc               *time.Location    // Location for time.Time values
	MaxAllowedPacket  int               // Max packet size allowed
	ServerPubKey      string            // Server public key name
	pubKey            *rsa.PublicKey    // Server public key
	TLSConfig         string            // TLS configuration name
	TLS               *tls.Config       // TLS configuration, its priority is higher than TLSConfig
	Timeout           time.Duration     // Dial timeout
	ReadTimeout       time.Duration     // I/O read timeout
	WriteTimeout      time.Duration     // I/O write timeout

	AllowAllFiles            bool // Allow all files to be used with LOAD DATA LOCAL INFILE
	AllowCleartextPasswords  bool // Allows the cleartext client side plugin
	AllowFallbackToPlaintext bool // Allows fallback to unencrypted connection if server does not support TLS
	AllowNativePasswords     bool // Allows the native password authentication method
	AllowOldPasswords        bool // Allows the old insecure password method
	CheckConnLiveness        bool // Check connections for liveness before using them
	ClientFoundRows          bool // Return number of matching rows instead of rows changed
	ColumnsWithAlias         bool // Prepend table alias to column names
	InterpolateParams        bool // Interpolate placeholders into query string
	MultiStatements          bool // Allow multiple statements in one query
	ParseTime                bool // Parse time values to time.Time
	RejectReadOnly           bool // Reject read-only connections
}

// NewConfig creates a new Config and sets default values.
func NewDsnConfig() *DsnConfig {
	return &DsnConfig{
		Loc:                  time.UTC,
		AllowNativePasswords: true,
		CheckConnLiveness:    true,
	}
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (cfg *DsnConfig, err error) {
	// New config with some default values
	cfg = NewDsnConfig()

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Passwd = dsn[k+1 : j]
								break
							}
						}
						cfg.User = dsn[:k]

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, errInvalidDSNUnescaped
							}
							return nil, errInvalidDSNAddr
						}
						cfg.Addr = dsn[k+1 : i-1]
						break
					}
				}
				cfg.Net = dsn[j+1 : k]
			}

			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}
			cfg.DBName = dsn[i+1 : j]

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}

	return cfg, nil
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *DsnConfig, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}

		// cfg params
		switch value := param[1]; param[0] {
		// Collation
		case "collation":
			cfg.Collation = value
		case "engine":
			cfg.Engine = value
		case "fetchSize":
			num, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return err
			}
			cfg.FetchSize = int32(num)
		case "enableStreamQuery":
			v, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			cfg.enableStreamQuery = v
		// Compression
		case "compress":
			return errors.New("compression not implemented yet")
		// Time Location
		case "loc":
			if value, err = url.QueryUnescape(value); err != nil {
				return
			}
			cfg.Loc, err = time.LoadLocation(value)
			if err != nil {
				return
			}
		// I/O read Timeout
		case "readTimeout":
			cfg.ReadTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}
		// Server public key
		case "serverPubKey":
			name, err := url.QueryUnescape(value)
			if err != nil {
				return fmt.Errorf("invalid value for server pub key name: %v", err)
			}
			cfg.ServerPubKey = name

		// Strict mode
		case "strict":
			panic("strict mode has been removed. See https://github.com/go-sql-driver/mysql/wiki/strict-mode")

		// Dial Timeout
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}

		// I/O write Timeout
		case "writeTimeout":
			cfg.WriteTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}
		case "maxAllowedPacket":
			cfg.MaxAllowedPacket, err = strconv.Atoi(value)
			if err != nil {
				return
			}
		default:
			// lazy init
			if cfg.Params == nil {
				cfg.Params = make(map[string]string)
			}

			if cfg.Params[param[0]], err = url.QueryUnescape(value); err != nil {
				return
			}
		}
	}

	return
}

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "3306")
	}
	return addr
}
