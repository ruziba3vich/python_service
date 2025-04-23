package config

import (
	"os"

	"github.com/joho/godotenv"
)

type (
	Config struct {
		KafkaCfg *KafkaConfig
	}

	KafkaConfig struct {
		Topic  string
		Broker string
	}
)

func NewConfig() *Config {
	_ = godotenv.Load()
	return &Config{
		KafkaCfg: &KafkaConfig{
			Topic:  getEnv("KAFKA_TOPIC", "logs"),
			Broker: getEnv("KAFKA_BROKER", "217.76.51.104:9092"),
		},
	}
}

// getEnv returns the fallback value if the given key is not provided in env
func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
