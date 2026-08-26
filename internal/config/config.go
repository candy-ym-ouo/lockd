package config

import (
	"flag"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr            string
	ForceToken      string
	Namespaces      []string
	NamespaceQuota  int
	DefaultTTL      time.Duration
	ExpireInterval  time.Duration
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
	LogLevel        string
}

func Default() Config {
	return Config{
		Addr:            "127.0.0.1:8080",
		ForceToken:      "",
		Namespaces:      nil,
		NamespaceQuota:  100000,
		DefaultTTL:      30 * time.Second,
		ExpireInterval:  500 * time.Millisecond,
		RequestTimeout:  10 * time.Minute,
		ShutdownTimeout: 5 * time.Second,
		LogLevel:        "info",
	}
}

func Parse(args []string) (Config, error) {
	cfg := Default()
	applyEnvironment(&cfg)
	set := flag.NewFlagSet("lockd", flag.ContinueOnError)
	set.StringVar(&cfg.Addr, "addr", cfg.Addr, "HTTP listen address")
	set.StringVar(&cfg.ForceToken, "force-token", cfg.ForceToken, "administrator force token")
	namespaces := strings.Join(cfg.Namespaces, ",")
	set.StringVar(&namespaces, "namespaces", namespaces, "allowed namespaces, comma separated")
	set.IntVar(&cfg.NamespaceQuota, "namespace-quota", cfg.NamespaceQuota, "maximum locks per namespace")
	set.DurationVar(&cfg.DefaultTTL, "default-ttl", cfg.DefaultTTL, "default lock lease")
	set.DurationVar(&cfg.ExpireInterval, "expire-interval", cfg.ExpireInterval, "lease scan interval")
	set.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "maximum acquire request duration")
	set.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "graceful shutdown limit")
	set.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug, info, warn, or error")
	if err := set.Parse(args); err != nil {
		return Config{}, err
	}
	cfg.Namespaces = splitList(namespaces)
	return cfg, nil
}
func applyEnvironment(cfg *Config) {
	setString("LOCKD_ADDR", &cfg.Addr)
	setString("LOCKD_FORCE_TOKEN", &cfg.ForceToken)
	setString("LOCKD_LOG_LEVEL", &cfg.LogLevel)
	if value := os.Getenv("LOCKD_NAMESPACES"); value != "" {
		cfg.Namespaces = splitList(value)
	}
	if value := os.Getenv("LOCKD_NAMESPACE_QUOTA"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.NamespaceQuota = parsed
		}
	}
	setDuration("LOCKD_DEFAULT_TTL", &cfg.DefaultTTL)
	setDuration("LOCKD_EXPIRE_INTERVAL", &cfg.ExpireInterval)
	setDuration("LOCKD_REQUEST_TIMEOUT", &cfg.RequestTimeout)
	setDuration("LOCKD_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout)
}
func setString(name string, target *string) {
	if value := os.Getenv(name); value != "" {
		*target = value
	}
}
func setDuration(name string, target *time.Duration) {
	if value := os.Getenv(name); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			*target = parsed
		}
	}
}
func splitList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if item := strings.TrimSpace(part); item != "" {
			result = append(result, item)
		}
	}
	return result
}
