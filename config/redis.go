package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// globalRedisClient 全局 Redis 客户端
	globalRedisClient *RedisClient
	redisOnce         sync.Once
	redisMutex        sync.RWMutex
)

// RedisClient Redis 客户端封装
type RedisClient struct {
	client *redis.Client
	ctx    context.Context
}

// NewRedisClient 创建 Redis 客户端
func NewRedisClient() (*RedisClient, error) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		// 默认使用 localhost:6379
		redisURL = "redis://localhost:6379"
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("解析 Redis URL 失败: %w", err)
	}

	client := redis.NewClient(opts)
	ctx := context.Background()

	// 测试连接
	_, err = client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("连接 Redis 失败: %w", err)
	}

	log.Printf("✅ Redis 连接成功: %s", redisURL)
	return &RedisClient{
		client: client,
		ctx:    ctx,
	}, nil
}

// InitGlobalRedis 初始化全局 Redis 客户端（单例模式）
func InitGlobalRedis() error {
	var err error
	redisOnce.Do(func() {
		redisURL := os.Getenv("REDIS_URL")
		if redisURL == "" {
			// 默认使用 localhost:6379
			redisURL = "redis://localhost:6379"
		}

		opts, parseErr := redis.ParseURL(redisURL)
		if parseErr != nil {
			err = fmt.Errorf("解析 Redis URL 失败: %w", parseErr)
			return
		}

		client := redis.NewClient(opts)
		ctx := context.Background()

		// 测试连接
		_, pingErr := client.Ping(ctx).Result()
		if pingErr != nil {
			err = fmt.Errorf("连接 Redis 失败: %w", pingErr)
			return
		}

		globalRedisClient = &RedisClient{
			client: client,
			ctx:    ctx,
		}
		log.Printf("✅ 全局 Redis 客户端初始化成功: %s", redisURL)
	})
	return err
}

// GetGlobalRedis 获取全局 Redis 客户端
func GetGlobalRedis() *RedisClient {
	redisMutex.RLock()
	defer redisMutex.RUnlock()
	return globalRedisClient
}

// SetGlobalRedis 设置全局 Redis 客户端（用于测试或手动设置）
func SetGlobalRedis(client *RedisClient) {
	redisMutex.Lock()
	defer redisMutex.Unlock()
	globalRedisClient = client
}

// Close 关闭 Redis 连接
func (r *RedisClient) Close() error {
	return r.client.Close()
}

// Get 获取值
func (r *RedisClient) Get(key string) (string, error) {
	return r.client.Get(r.ctx, key).Result()
}

// Set 设置值
func (r *RedisClient) Set(key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(r.ctx, key, value, expiration).Err()
}

// Del 删除键
func (r *RedisClient) Del(key string) error {
	return r.client.Del(r.ctx, key).Err()
}

// Exists 检查键是否存在
func (r *RedisClient) Exists(key string) (bool, error) {
	count, err := r.client.Exists(r.ctx, key).Result()
	return count > 0, err
}

// Incr 递增
func (r *RedisClient) Incr(key string) (int64, error) {
	return r.client.Incr(r.ctx, key).Result()
}

// GetInt64 获取整数值
func (r *RedisClient) GetInt64(key string) (int64, error) {
	val, err := r.client.Get(r.ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil // 键不存在时返回 0，不视为错误
	}
	return val, err
}

// SetInt64 设置整数值
func (r *RedisClient) SetInt64(key string, value int64, expiration time.Duration) error {
	return r.client.Set(r.ctx, key, value, expiration).Err()
}
