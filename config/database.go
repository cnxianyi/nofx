package config

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"log"
	"nofx/crypto"
	"nofx/market"
	"os"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DatabaseInterface 定义了数据库实现需要提供的方法集合
type DatabaseInterface interface {
	SetCryptoService(cs *crypto.CryptoService)
	CreateUser(user *User) error
	GetUserByEmail(email string) (*User, error)
	GetUserByID(userID string) (*User, error)
	GetAllUsers() ([]string, error)
	UpdateUserOTPVerified(userID string, verified bool) error
	GetAIModels(userID string) ([]*AIModelConfig, error)
	UpdateAIModel(userID, id string, enabled bool, apiKey, customAPIURL, customModelName string) error
	GetExchanges(userID string) ([]*ExchangeConfig, error)
	UpdateExchange(userID, id string, enabled bool, apiKey, secretKey string, testnet bool, hyperliquidWalletAddr, asterUser, asterSigner, asterPrivateKey string) error
	CreateAIModel(userID, id, name, provider string, enabled bool, apiKey, customAPIURL string) error
	CreateExchange(userID, id, name, typ string, enabled bool, apiKey, secretKey string, testnet bool, hyperliquidWalletAddr, asterUser, asterSigner, asterPrivateKey string) error
	CreateTrader(trader *TraderRecord) error
	GetTraders(userID string) ([]*TraderRecord, error)
	UpdateTraderStatus(userID, id string, isRunning bool) error
	UpdateTrader(trader *TraderRecord) error
	UpdateTraderInitialBalance(userID, id string, newBalance float64) error
	UpdateTraderCustomPrompt(userID, id string, customPrompt string, overrideBase bool) error
	DeleteTrader(userID, id string) error
	GetTraderConfig(userID, traderID string) (*TraderRecord, *AIModelConfig, *ExchangeConfig, error)
	GetSystemConfig(key string) (string, error)
	SetSystemConfig(key, value string) error
	CreateUserSignalSource(userID, coinPoolURL, oiTopURL string) error
	GetUserSignalSource(userID string) (*UserSignalSource, error)
	UpdateUserSignalSource(userID, coinPoolURL, oiTopURL string) error
	GetCustomCoins() []string
	GetAllTimeframes() []string
	LoadBetaCodesFromFile(filePath string) error
	ValidateBetaCode(code string) (bool, error)
	UseBetaCode(code, userEmail string) error
	GetBetaCodeStats() (total, used int, err error)
	SaveDecisionLog(userID, traderID string, record interface{}) error
	GetDecisionLogs(userID, traderID string, limit int) ([]bson.M, error)
	Close() error
}

// Database 配置数据库
type Database struct {
	client        *mongo.Client
	db            *mongo.Database
	cryptoService *crypto.CryptoService
	ctx           context.Context
}

// NewDatabase 创建配置数据库
// dbPath 现在接受 MongoDB 连接字符串，例如: "mongodb://localhost:27017" 或 "mongodb://user:pass@host:port/dbname"
func NewDatabase(dbPath string) (*Database, error) {
	ctx := context.Background()

	// 解析连接字符串，提取数据库名称
	// MongoDB URI 格式: mongodb://[username:password@]host[:port][/database][?options]
	dbName := "nofx"
	uri := dbPath

	// 如果连接字符串中包含数据库名称，提取它
	if strings.Contains(uri, "/") && !strings.HasSuffix(uri, "/") {
		// 找到最后一个 / 之后的部分（排除查询参数）
		parts := strings.Split(uri, "?")
		pathPart := parts[0]
		pathParts := strings.Split(pathPart, "/")
		if len(pathParts) > 3 {
			// mongodb://host:port/dbname 格式
			lastPart := pathParts[len(pathParts)-1]
			if lastPart != "" && !strings.Contains(lastPart, "@") && !strings.Contains(lastPart, ":") {
				dbName = lastPart
				// 从 URI 中移除数据库名称，保留查询参数
				if len(parts) > 1 {
					uri = strings.TrimSuffix(pathPart, "/"+dbName) + "?" + parts[1]
				} else {
					uri = strings.TrimSuffix(pathPart, "/"+dbName)
				}
			}
		}
	}

	// 设置客户端选项
	clientOptions := options.Client().ApplyURI(uri)

	// 连接到 MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("连接 MongoDB 失败: %w", err)
	}

	// 测试连接
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("测试 MongoDB 连接失败: %w", err)
	}

	database := &Database{
		client: client,
		db:     client.Database(dbName),
		ctx:    ctx,
	}

	// 创建索引
	if err := database.createIndexes(); err != nil {
		return nil, fmt.Errorf("创建索引失败: %w", err)
	}

	// 初始化默认数据
	if err := database.initDefaultData(); err != nil {
		return nil, fmt.Errorf("初始化默认数据失败: %w", err)
	}

	log.Printf("✅ MongoDB 数据库连接成功: %s/%s", uri, dbName)
	return database, nil
}

// createIndexes 创建数据库索引
func (d *Database) createIndexes() error {
	// users 集合索引
	usersCollection := d.db.Collection("users")
	_, err := usersCollection.Indexes().CreateMany(d.ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "email", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	if err != nil {
		return fmt.Errorf("创建 users 索引失败: %w", err)
	}

	// ai_models 集合索引
	aiModelsCollection := d.db.Collection("ai_models")
	_, err = aiModelsCollection.Indexes().CreateMany(d.ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "user_id", Value: 1}}},
		{Keys: bson.D{{Key: "model_id", Value: 1}, {Key: "user_id", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	if err != nil {
		return fmt.Errorf("创建 ai_models 索引失败: %w", err)
	}

	// exchanges 集合索引
	exchangesCollection := d.db.Collection("exchanges")
	_, err = exchangesCollection.Indexes().CreateMany(d.ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "user_id", Value: 1}}},
		{Keys: bson.D{{Key: "exchange_id", Value: 1}, {Key: "user_id", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	if err != nil {
		return fmt.Errorf("创建 exchanges 索引失败: %w", err)
	}

	// traders 集合索引
	tradersCollection := d.db.Collection("traders")
	_, err = tradersCollection.Indexes().CreateMany(d.ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "user_id", Value: 1}}},
	})
	if err != nil {
		return fmt.Errorf("创建 traders 索引失败: %w", err)
	}

	// user_signal_sources 集合索引
	signalSourcesCollection := d.db.Collection("user_signal_sources")
	_, err = signalSourcesCollection.Indexes().CreateMany(d.ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "user_id", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	if err != nil {
		return fmt.Errorf("创建 user_signal_sources 索引失败: %w", err)
	}

	// system_config 集合索引
	systemConfigCollection := d.db.Collection("system_config")
	_, err = systemConfigCollection.Indexes().CreateMany(d.ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "key", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	if err != nil {
		return fmt.Errorf("创建 system_config 索引失败: %w", err)
	}

	// beta_codes 集合索引
	betaCodesCollection := d.db.Collection("beta_codes")
	_, err = betaCodesCollection.Indexes().CreateMany(d.ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "code", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	if err != nil {
		return fmt.Errorf("创建 beta_codes 索引失败: %w", err)
	}

	return nil
}

// initDefaultData 初始化默认数据
func (d *Database) initDefaultData() error {
	aiModelsCollection := d.db.Collection("ai_models")
	exchangesCollection := d.db.Collection("exchanges")
	systemConfigCollection := d.db.Collection("system_config")

	// 初始化AI模型（使用default用户）
	aiModels := []struct {
		modelID, name, provider string
	}{
		{"deepseek", "DeepSeek", "deepseek"},
		{"qwen", "Qwen", "qwen"},
	}

	for _, model := range aiModels {
		// 检查是否已存在
		filter := bson.M{"model_id": model.modelID, "user_id": "default"}
		count, err := aiModelsCollection.CountDocuments(d.ctx, filter)
		if err != nil {
			return fmt.Errorf("检查AI模型失败: %w", err)
		}

		if count == 0 {
			// 生成自增ID（使用计数器集合）
			id, err := d.getNextSequence("ai_models")
			if err != nil {
				return fmt.Errorf("获取AI模型ID失败: %w", err)
			}

			doc := bson.M{
				"id":                id,
				"model_id":          model.modelID,
				"user_id":           "default",
				"name":              model.name,
				"provider":          model.provider,
				"enabled":           false,
				"api_key":           "",
				"custom_api_url":    "",
				"custom_model_name": "",
				"created_at":        time.Now(),
				"updated_at":        time.Now(),
			}
			_, err = aiModelsCollection.InsertOne(d.ctx, doc)
			if err != nil {
				return fmt.Errorf("初始化AI模型失败: %w", err)
			}
		}
	}

	// 初始化交易所（使用default用户）
	exchanges := []struct {
		exchangeID, name, typ string
	}{
		{"binance", "Binance Futures", "binance"},
		{"hyperliquid", "Hyperliquid", "hyperliquid"},
		{"aster", "Aster DEX", "aster"},
	}

	for _, exchange := range exchanges {
		// 检查是否已存在
		filter := bson.M{"exchange_id": exchange.exchangeID, "user_id": "default"}
		count, err := exchangesCollection.CountDocuments(d.ctx, filter)
		if err != nil {
			return fmt.Errorf("检查交易所失败: %w", err)
		}

		if count == 0 {
			// 生成自增ID
			id, err := d.getNextSequence("exchanges")
			if err != nil {
				return fmt.Errorf("获取交易所ID失败: %w", err)
			}

			doc := bson.M{
				"id":                      id,
				"exchange_id":             exchange.exchangeID,
				"user_id":                 "default",
				"name":                    exchange.name,
				"type":                    exchange.typ,
				"enabled":                 false,
				"api_key":                 "",
				"secret_key":              "",
				"testnet":                 false,
				"hyperliquid_wallet_addr": "",
				"aster_user":              "",
				"aster_signer":            "",
				"aster_private_key":       "",
				"created_at":              time.Now(),
				"updated_at":              time.Now(),
			}
			_, err = exchangesCollection.InsertOne(d.ctx, doc)
			if err != nil {
				return fmt.Errorf("初始化交易所失败: %w", err)
			}
		}
	}

	// 初始化系统配置
	systemConfigs := map[string]string{
		"beta_mode":            "false",
		"api_server_port":      "8080",
		"use_default_coins":    "true",
		"default_coins":        `["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","DOGEUSDT","ADAUSDT","HYPEUSDT"]`,
		"max_daily_loss":       "10.0",
		"max_drawdown":         "20.0",
		"stop_trading_minutes": "60",
		"btc_eth_leverage":     "5",
		"altcoin_leverage":     "5",
		"jwt_secret":           "",
		"registration_enabled": "true",
	}

	for key, value := range systemConfigs {
		filter := bson.M{"key": key}
		update := bson.M{
			"$setOnInsert": bson.M{
				"key":        key,
				"value":      value,
				"updated_at": time.Now(),
			},
		}
		opts := options.Update().SetUpsert(true)
		_, err := systemConfigCollection.UpdateOne(d.ctx, filter, update, opts)
		if err != nil {
			return fmt.Errorf("初始化系统配置失败: %w", err)
		}
	}

	return nil
}

// getNextSequence 获取下一个自增序列号
func (d *Database) getNextSequence(collectionName string) (int, error) {
	countersCollection := d.db.Collection("counters")
	filter := bson.M{"_id": collectionName}
	update := bson.M{
		"$inc": bson.M{"seq": 1},
	}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	var result struct {
		ID  string `bson:"_id"`
		Seq int    `bson:"seq"`
	}
	err := countersCollection.FindOneAndUpdate(d.ctx, filter, update, opts).Decode(&result)
	if err != nil {
		return 0, err
	}
	return result.Seq, nil
}

// User 用户配置
type User struct {
	ID           string    `json:"id" bson:"id"`
	Email        string    `json:"email" bson:"email"`
	PasswordHash string    `json:"-" bson:"password_hash"` // 不返回到前端
	OTPSecret    string    `json:"-" bson:"otp_secret"`    // 不返回到前端
	OTPVerified  bool      `json:"otp_verified" bson:"otp_verified"`
	CreatedAt    time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt    time.Time `json:"updated_at" bson:"updated_at"`
}

// AIModelConfig AI模型配置
type AIModelConfig struct {
	ID              int       `json:"id" bson:"id"`             // 自增ID（主键）
	ModelID         string    `json:"model_id" bson:"model_id"` // 模型类型ID（例如 "deepseek"）
	UserID          string    `json:"user_id" bson:"user_id"`
	DisplayName     string    `json:"display_name" bson:"display_name"` // 用户自定义显示名称
	Name            string    `json:"name" bson:"name"`
	Provider        string    `json:"provider" bson:"provider"`
	Enabled         bool      `json:"enabled" bson:"enabled"`
	APIKey          string    `json:"apiKey" bson:"api_key"`
	CustomAPIURL    string    `json:"customApiUrl" bson:"custom_api_url"`
	CustomModelName string    `json:"customModelName" bson:"custom_model_name"`
	CreatedAt       time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt       time.Time `json:"updated_at" bson:"updated_at"`
}

// ExchangeConfig 交易所配置
type ExchangeConfig struct {
	ID          int    `json:"id" bson:"id"`                   // 自增ID（主键）
	ExchangeID  string `json:"exchange_id" bson:"exchange_id"` // 交易所类型ID（例如 "binance"）
	UserID      string `json:"user_id" bson:"user_id"`
	DisplayName string `json:"display_name" bson:"display_name"` // 用户自定义显示名称
	Name        string `json:"name" bson:"name"`
	Type        string `json:"type" bson:"type"`
	Enabled     bool   `json:"enabled" bson:"enabled"`
	APIKey      string `json:"apiKey" bson:"api_key"`       // For Binance: API Key; For Hyperliquid: Agent Private Key (should have ~0 balance)
	SecretKey   string `json:"secretKey" bson:"secret_key"` // For Binance: Secret Key; Not used for Hyperliquid
	Testnet     bool   `json:"testnet" bson:"testnet"`
	// Hyperliquid Agent Wallet configuration (following official best practices)
	// Reference: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets
	HyperliquidWalletAddr string `json:"hyperliquidWalletAddr" bson:"hyperliquid_wallet_addr"` // Main Wallet Address (holds funds, never expose private key)
	// Aster 特定字段
	AsterUser       string    `json:"asterUser" bson:"aster_user"`
	AsterSigner     string    `json:"asterSigner" bson:"aster_signer"`
	AsterPrivateKey string    `json:"asterPrivateKey" bson:"aster_private_key"`
	CreatedAt       time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt       time.Time `json:"updated_at" bson:"updated_at"`
}

// TraderRecord 交易员配置（数据库实体）
type TraderRecord struct {
	ID                   string    `json:"id" bson:"id"`
	UserID               string    `json:"user_id" bson:"user_id"`
	Name                 string    `json:"name" bson:"name"`
	AIModelID            int       `json:"ai_model_id" bson:"ai_model_id"` // 外键：指向 ai_models.id
	ExchangeID           int       `json:"exchange_id" bson:"exchange_id"` // 外键：指向 exchanges.id
	InitialBalance       float64   `json:"initial_balance" bson:"initial_balance"`
	ScanIntervalMinutes  int       `json:"scan_interval_minutes" bson:"scan_interval_minutes"`
	IsRunning            bool      `json:"is_running" bson:"is_running"`
	BTCETHLeverage       int       `json:"btc_eth_leverage" bson:"btc_eth_leverage"`             // BTC/ETH杠杆倍数
	AltcoinLeverage      int       `json:"altcoin_leverage" bson:"altcoin_leverage"`             // 山寨币杠杆倍数
	TradingSymbols       string    `json:"trading_symbols" bson:"trading_symbols"`               // 交易币种，逗号分隔
	UseCoinPool          bool      `json:"use_coin_pool" bson:"use_coin_pool"`                   // 是否使用COIN POOL信号源
	UseOITop             bool      `json:"use_oi_top" bson:"use_oi_top"`                         // 是否使用OI TOP信号源
	CustomPrompt         string    `json:"custom_prompt" bson:"custom_prompt"`                   // 自定义交易策略prompt
	OverrideBasePrompt   bool      `json:"override_base_prompt" bson:"override_base_prompt"`     // 是否覆盖基础prompt
	SystemPromptTemplate string    `json:"system_prompt_template" bson:"system_prompt_template"` // 系统提示词模板名称
	IsCrossMargin        bool      `json:"is_cross_margin" bson:"is_cross_margin"`               // 是否为全仓模式（true=全仓，false=逐仓）
	TakerFeeRate         float64   `json:"taker_fee_rate" bson:"taker_fee_rate"`                 // Taker fee rate, default 0.0004
	MakerFeeRate         float64   `json:"maker_fee_rate" bson:"maker_fee_rate"`                 // Maker fee rate, default 0.0002
	OrderStrategy        string    `json:"order_strategy" bson:"order_strategy"`                 // Order strategy: "market_only", "conservative_hybrid", "limit_only"
	LimitPriceOffset     float64   `json:"limit_price_offset" bson:"limit_price_offset"`         // Limit order price offset percentage (e.g., -0.03 for -0.03%)
	LimitTimeoutSeconds  int       `json:"limit_timeout_seconds" bson:"limit_timeout_seconds"`   // Timeout in seconds before converting to market order (default: 60)
	Timeframes           string    `json:"timeframes" bson:"timeframes"`                         // 时间线选择 (逗号分隔，例如: "1m,4h,1d")
	CreatedAt            time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt            time.Time `json:"updated_at" bson:"updated_at"`
}

// UserSignalSource 用户信号源配置
type UserSignalSource struct {
	ID          int       `json:"id" bson:"id"`
	UserID      string    `json:"user_id" bson:"user_id"`
	CoinPoolURL string    `json:"coin_pool_url" bson:"coin_pool_url"`
	OITopURL    string    `json:"oi_top_url" bson:"oi_top_url"`
	CreatedAt   time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" bson:"updated_at"`
}

// GenerateOTPSecret 生成OTP密钥
func GenerateOTPSecret() (string, error) {
	secret := make([]byte, 20)
	_, err := rand.Read(secret)
	if err != nil {
		return "", err
	}
	return base32.StdEncoding.EncodeToString(secret), nil
}

// CreateUser 创建用户
func (d *Database) CreateUser(user *User) error {
	collection := d.db.Collection("users")
	doc := bson.M{
		"id":            user.ID,
		"email":         user.Email,
		"password_hash": user.PasswordHash,
		"otp_secret":    user.OTPSecret,
		"otp_verified":  user.OTPVerified,
		"created_at":    time.Now(),
		"updated_at":    time.Now(),
	}
	_, err := collection.InsertOne(d.ctx, doc)
	return err
}

// EnsureAdminUser 确保admin用户存在（用于管理员模式）
func (d *Database) EnsureAdminUser() error {
	collection := d.db.Collection("users")
	filter := bson.M{"id": "admin"}
	count, err := collection.CountDocuments(d.ctx, filter)
	if err != nil {
		return err
	}

	// 如果已存在，直接返回
	if count > 0 {
		return nil
	}

	// 创建admin用户（密码为空，因为管理员模式下不需要密码）
	adminUser := &User{
		ID:           "admin",
		Email:        "admin@localhost",
		PasswordHash: "", // 管理员模式下不使用密码
		OTPSecret:    "",
		OTPVerified:  true,
	}

	return d.CreateUser(adminUser)
}

// GetUserByEmail 通过邮箱获取用户
func (d *Database) GetUserByEmail(email string) (*User, error) {
	collection := d.db.Collection("users")
	filter := bson.M{"email": email}
	var user User
	err := collection.FindOne(d.ctx, filter).Decode(&user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

// GetUserByID 通过ID获取用户
func (d *Database) GetUserByID(userID string) (*User, error) {
	collection := d.db.Collection("users")
	filter := bson.M{"id": userID}
	var user User
	err := collection.FindOne(d.ctx, filter).Decode(&user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

// GetAllUsers 获取所有用户ID列表
func (d *Database) GetAllUsers() ([]string, error) {
	collection := d.db.Collection("users")
	cursor, err := collection.Find(d.ctx, bson.M{}, options.Find().SetSort(bson.M{"id": 1}).SetProjection(bson.M{"id": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(d.ctx)

	var userIDs []string
	for cursor.Next(d.ctx) {
		var result struct {
			ID string `bson:"id"`
		}
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		userIDs = append(userIDs, result.ID)
	}
	return userIDs, nil
}

// UpdateUserOTPVerified 更新用户OTP验证状态
func (d *Database) UpdateUserOTPVerified(userID string, verified bool) error {
	collection := d.db.Collection("users")
	filter := bson.M{"id": userID}
	update := bson.M{"$set": bson.M{"otp_verified": verified, "updated_at": time.Now()}}
	_, err := collection.UpdateOne(d.ctx, filter, update)
	return err
}

// UpdateUserPassword 更新用户密码
func (d *Database) UpdateUserPassword(userID, passwordHash string) error {
	collection := d.db.Collection("users")
	filter := bson.M{"id": userID}
	update := bson.M{"$set": bson.M{"password_hash": passwordHash, "updated_at": time.Now()}}
	_, err := collection.UpdateOne(d.ctx, filter, update)
	return err
}

// GetAIModels 获取用户的AI模型配置
func (d *Database) GetAIModels(userID string) ([]*AIModelConfig, error) {
	collection := d.db.Collection("ai_models")
	filter := bson.M{"user_id": userID}
	cursor, err := collection.Find(d.ctx, filter, options.Find().SetSort(bson.M{"id": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(d.ctx)

	models := make([]*AIModelConfig, 0)
	for cursor.Next(d.ctx) {
		var model AIModelConfig
		if err := cursor.Decode(&model); err != nil {
			return nil, err
		}
		// 处理空值
		if model.CustomAPIURL == "" {
			model.CustomAPIURL = ""
		}
		if model.CustomModelName == "" {
			model.CustomModelName = ""
		}
		// 解密API Key
		model.APIKey = d.decryptSensitiveData(model.APIKey)
		models = append(models, &model)
	}

	return models, nil
}

// UpdateAIModel 更新AI模型配置，如果不存在则创建用户特定配置
func (d *Database) UpdateAIModel(userID, id string, enabled bool, apiKey, customAPIURL, customModelName string) error {
	collection := d.db.Collection("ai_models")

	// 先尝试精确匹配 model_id
	filter := bson.M{"user_id": userID, "model_id": id}
	var existingModel bson.M
	err := collection.FindOne(d.ctx, filter).Decode(&existingModel)

	if err == nil {
		// 找到了现有配置，更新它
		encryptedAPIKey := d.encryptSensitiveData(apiKey)
		update := bson.M{
			"$set": bson.M{
				"enabled":           enabled,
				"api_key":           encryptedAPIKey,
				"custom_api_url":    customAPIURL,
				"custom_model_name": customModelName,
				"updated_at":        time.Now(),
			},
		}
		_, err = collection.UpdateOne(d.ctx, filter, update)
		return err
	}

	// model_id 不存在，尝试兼容旧逻辑：将 id 作为 provider 查找
	provider := id
	filter = bson.M{"user_id": userID, "provider": provider}
	err = collection.FindOne(d.ctx, filter).Decode(&existingModel)

	if err == nil {
		// 找到了现有配置（通过 provider 匹配，兼容旧版），更新它
		modelID, _ := existingModel["model_id"].(string)
		log.Printf("⚠️  使用旧版 provider 匹配更新模型: %s -> %s", provider, modelID)
		encryptedAPIKey := d.encryptSensitiveData(apiKey)
		update := bson.M{
			"$set": bson.M{
				"enabled":           enabled,
				"api_key":           encryptedAPIKey,
				"custom_api_url":    customAPIURL,
				"custom_model_name": customModelName,
				"updated_at":        time.Now(),
			},
		}
		_, err = collection.UpdateOne(d.ctx, bson.M{"user_id": userID, "model_id": modelID}, update)
		return err
	}

	// 没有找到任何现有配置，创建新的
	// 推断 provider
	if provider == id && (provider == "deepseek" || provider == "qwen") {
		provider = id
	} else {
		parts := strings.Split(id, "_")
		if len(parts) >= 2 {
			provider = parts[len(parts)-1]
		} else {
			provider = id
		}
	}

	// 获取模型的基本信息
	var name string
	filter = bson.M{"provider": provider}
	var modelInfo bson.M
	err = collection.FindOne(d.ctx, filter).Decode(&modelInfo)
	if err == nil {
		name, _ = modelInfo["name"].(string)
	}
	if name == "" {
		// 使用默认值
		if provider == "deepseek" {
			name = "DeepSeek AI"
		} else if provider == "qwen" {
			name = "Qwen AI"
		} else {
			name = provider + " AI"
		}
	}

	// 生成新的 ID
	newModelID := id
	if id == provider {
		newModelID = fmt.Sprintf("%s_%s", userID, provider)
	}

	log.Printf("✓ 创建新的 AI 模型配置: ID=%s, Provider=%s, Name=%s", newModelID, provider, name)
	encryptedAPIKey := d.encryptSensitiveData(apiKey)

	// 生成自增ID
	modelID, err := d.getNextSequence("ai_models")
	if err != nil {
		return fmt.Errorf("获取AI模型ID失败: %w", err)
	}

	doc := bson.M{
		"id":                modelID,
		"model_id":          newModelID,
		"user_id":           userID,
		"name":              name,
		"provider":          provider,
		"enabled":           enabled,
		"api_key":           encryptedAPIKey,
		"custom_api_url":    customAPIURL,
		"custom_model_name": customModelName,
		"created_at":        time.Now(),
		"updated_at":        time.Now(),
	}
	_, err = collection.InsertOne(d.ctx, doc)
	return err
}

// GetExchanges 获取用户的交易所配置
func (d *Database) GetExchanges(userID string) ([]*ExchangeConfig, error) {
	collection := d.db.Collection("exchanges")
	filter := bson.M{"user_id": userID}
	cursor, err := collection.Find(d.ctx, filter, options.Find().SetSort(bson.M{"id": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(d.ctx)

	exchanges := make([]*ExchangeConfig, 0)
	for cursor.Next(d.ctx) {
		var exchange ExchangeConfig
		if err := cursor.Decode(&exchange); err != nil {
			return nil, err
		}
		// 处理空值
		if exchange.HyperliquidWalletAddr == "" {
			exchange.HyperliquidWalletAddr = ""
		}
		if exchange.AsterUser == "" {
			exchange.AsterUser = ""
		}
		if exchange.AsterSigner == "" {
			exchange.AsterSigner = ""
		}
		if exchange.AsterPrivateKey == "" {
			exchange.AsterPrivateKey = ""
		}
		// 解密敏感字段
		exchange.APIKey = d.decryptSensitiveData(exchange.APIKey)
		exchange.SecretKey = d.decryptSensitiveData(exchange.SecretKey)
		exchange.AsterPrivateKey = d.decryptSensitiveData(exchange.AsterPrivateKey)
		exchanges = append(exchanges, &exchange)
	}

	return exchanges, nil
}

// UpdateExchange 更新交易所配置，如果不存在则创建用户特定配置
// 🔒 安全特性：空值不会覆盖现有的敏感字段（api_key, secret_key, aster_private_key）
func (d *Database) UpdateExchange(userID, id string, enabled bool, apiKey, secretKey string, testnet bool, hyperliquidWalletAddr, asterUser, asterSigner, asterPrivateKey string) error {
	log.Printf("🔧 UpdateExchange: userID=%s, id=%s, enabled=%v", userID, id, enabled)
	collection := d.db.Collection("exchanges")
	filter := bson.M{"exchange_id": id, "user_id": userID}

	// 构建更新文档
	update := bson.M{
		"$set": bson.M{
			"enabled":                 enabled,
			"testnet":                 testnet,
			"hyperliquid_wallet_addr": hyperliquidWalletAddr,
			"aster_user":              asterUser,
			"aster_signer":            asterSigner,
			"updated_at":              time.Now(),
		},
	}

	// 🔒 敏感字段：只在非空时更新（保护现有数据）
	if apiKey != "" {
		encryptedAPIKey := d.encryptSensitiveData(apiKey)
		update["$set"].(bson.M)["api_key"] = encryptedAPIKey
	}
	if secretKey != "" {
		encryptedSecretKey := d.encryptSensitiveData(secretKey)
		update["$set"].(bson.M)["secret_key"] = encryptedSecretKey
	}
	if asterPrivateKey != "" {
		encryptedAsterPrivateKey := d.encryptSensitiveData(asterPrivateKey)
		update["$set"].(bson.M)["aster_private_key"] = encryptedAsterPrivateKey
	}

	// 执行更新
	result, err := collection.UpdateOne(d.ctx, filter, update)
	if err != nil {
		log.Printf("❌ UpdateExchange: 更新失败: %v", err)
		return err
	}

	log.Printf("📊 UpdateExchange: 影响行数 = %d", result.ModifiedCount)

	// 如果没有行被更新，说明用户没有这个交易所的配置，需要创建
	if result.MatchedCount == 0 {
		log.Printf("💡 UpdateExchange: 没有现有记录，创建新记录")

		// 根据交易所ID确定基本信息
		var name, typ string
		if id == "binance" {
			name = "Binance Futures"
			typ = "cex"
		} else if id == "hyperliquid" {
			name = "Hyperliquid"
			typ = "dex"
		} else if id == "aster" {
			name = "Aster DEX"
			typ = "dex"
		} else {
			name = id + " Exchange"
			typ = "cex"
		}

		log.Printf("🆕 UpdateExchange: 创建新记录 ID=%s, name=%s, type=%s", id, name, typ)

		// 生成自增ID
		exchangeID, err := d.getNextSequence("exchanges")
		if err != nil {
			return fmt.Errorf("获取交易所ID失败: %w", err)
		}

		// 加密敏感字段
		encryptedAPIKey := d.encryptSensitiveData(apiKey)
		encryptedSecretKey := d.encryptSensitiveData(secretKey)
		encryptedAsterPrivateKey := d.encryptSensitiveData(asterPrivateKey)

		doc := bson.M{
			"id":                      exchangeID,
			"exchange_id":             id,
			"user_id":                 userID,
			"name":                    name,
			"type":                    typ,
			"enabled":                 enabled,
			"api_key":                 encryptedAPIKey,
			"secret_key":              encryptedSecretKey,
			"testnet":                 testnet,
			"hyperliquid_wallet_addr": hyperliquidWalletAddr,
			"aster_user":              asterUser,
			"aster_signer":            asterSigner,
			"aster_private_key":       encryptedAsterPrivateKey,
			"created_at":              time.Now(),
			"updated_at":              time.Now(),
		}
		_, err = collection.InsertOne(d.ctx, doc)

		if err != nil {
			log.Printf("❌ UpdateExchange: 创建记录失败: %v", err)
		} else {
			log.Printf("✅ UpdateExchange: 创建记录成功")
		}
		return err
	}

	log.Printf("✅ UpdateExchange: 更新现有记录成功")
	return nil
}

// CreateAIModel 创建AI模型配置
func (d *Database) CreateAIModel(userID, id, name, provider string, enabled bool, apiKey, customAPIURL string) error {
	collection := d.db.Collection("ai_models")

	// 检查是否已存在
	filter := bson.M{"model_id": id, "user_id": userID}
	count, err := collection.CountDocuments(d.ctx, filter)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil // 已存在，忽略
	}

	// 生成自增ID
	modelID, err := d.getNextSequence("ai_models")
	if err != nil {
		return fmt.Errorf("获取AI模型ID失败: %w", err)
	}

	encryptedAPIKey := d.encryptSensitiveData(apiKey)
	doc := bson.M{
		"id":             modelID,
		"model_id":       id,
		"user_id":        userID,
		"name":           name,
		"provider":       provider,
		"enabled":        enabled,
		"api_key":        encryptedAPIKey,
		"custom_api_url": customAPIURL,
		"created_at":     time.Now(),
		"updated_at":     time.Now(),
	}
	_, err = collection.InsertOne(d.ctx, doc)
	return err
}

// CreateExchange 创建交易所配置
func (d *Database) CreateExchange(userID, id, name, typ string, enabled bool, apiKey, secretKey string, testnet bool, hyperliquidWalletAddr, asterUser, asterSigner, asterPrivateKey string) error {
	collection := d.db.Collection("exchanges")

	// 检查是否已存在
	filter := bson.M{"exchange_id": id, "user_id": userID}
	count, err := collection.CountDocuments(d.ctx, filter)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil // 已存在，忽略
	}

	// 生成自增ID
	exchangeID, err := d.getNextSequence("exchanges")
	if err != nil {
		return fmt.Errorf("获取交易所ID失败: %w", err)
	}

	// 加密敏感字段
	encryptedAPIKey := d.encryptSensitiveData(apiKey)
	encryptedSecretKey := d.encryptSensitiveData(secretKey)
	encryptedAsterPrivateKey := d.encryptSensitiveData(asterPrivateKey)

	doc := bson.M{
		"id":                      exchangeID,
		"exchange_id":             id,
		"user_id":                 userID,
		"name":                    name,
		"type":                    typ,
		"enabled":                 enabled,
		"api_key":                 encryptedAPIKey,
		"secret_key":              encryptedSecretKey,
		"testnet":                 testnet,
		"hyperliquid_wallet_addr": hyperliquidWalletAddr,
		"aster_user":              asterUser,
		"aster_signer":            asterSigner,
		"aster_private_key":       encryptedAsterPrivateKey,
		"created_at":              time.Now(),
		"updated_at":              time.Now(),
	}
	_, err = collection.InsertOne(d.ctx, doc)
	return err
}

// CreateTrader 创建交易员
func (d *Database) CreateTrader(trader *TraderRecord) error {
	collection := d.db.Collection("traders")
	doc := bson.M{
		"id":                     trader.ID,
		"user_id":                trader.UserID,
		"name":                   trader.Name,
		"ai_model_id":            trader.AIModelID,
		"exchange_id":            trader.ExchangeID,
		"initial_balance":        trader.InitialBalance,
		"scan_interval_minutes":  trader.ScanIntervalMinutes,
		"is_running":             trader.IsRunning,
		"btc_eth_leverage":       trader.BTCETHLeverage,
		"altcoin_leverage":       trader.AltcoinLeverage,
		"trading_symbols":        trader.TradingSymbols,
		"use_coin_pool":          trader.UseCoinPool,
		"use_oi_top":             trader.UseOITop,
		"custom_prompt":          trader.CustomPrompt,
		"override_base_prompt":   trader.OverrideBasePrompt,
		"system_prompt_template": trader.SystemPromptTemplate,
		"is_cross_margin":        trader.IsCrossMargin,
		"taker_fee_rate":         trader.TakerFeeRate,
		"maker_fee_rate":         trader.MakerFeeRate,
		"order_strategy":         trader.OrderStrategy,
		"limit_price_offset":     trader.LimitPriceOffset,
		"limit_timeout_seconds":  trader.LimitTimeoutSeconds,
		"timeframes":             trader.Timeframes,
		"created_at":             time.Now(),
		"updated_at":             time.Now(),
	}
	_, err := collection.InsertOne(d.ctx, doc)
	return err
}

// GetTraders 获取用户的交易员
func (d *Database) GetTraders(userID string) ([]*TraderRecord, error) {
	collection := d.db.Collection("traders")
	filter := bson.M{"user_id": userID}
	cursor, err := collection.Find(d.ctx, filter, options.Find().SetSort(bson.M{"created_at": -1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(d.ctx)

	var traders []*TraderRecord
	for cursor.Next(d.ctx) {
		var trader TraderRecord
		if err := cursor.Decode(&trader); err != nil {
			return nil, err
		}
		// 设置默认值
		if trader.BTCETHLeverage == 0 {
			trader.BTCETHLeverage = 5
		}
		if trader.AltcoinLeverage == 0 {
			trader.AltcoinLeverage = 5
		}
		if trader.TradingSymbols == "" {
			trader.TradingSymbols = ""
		}
		if trader.SystemPromptTemplate == "" {
			trader.SystemPromptTemplate = "default"
		}
		if trader.TakerFeeRate == 0 {
			trader.TakerFeeRate = 0.0004
		}
		if trader.MakerFeeRate == 0 {
			trader.MakerFeeRate = 0.0002
		}
		if trader.OrderStrategy == "" {
			trader.OrderStrategy = "conservative_hybrid"
		}
		if trader.LimitPriceOffset == 0 {
			trader.LimitPriceOffset = -0.03
		}
		if trader.LimitTimeoutSeconds == 0 {
			trader.LimitTimeoutSeconds = 60
		}
		if trader.Timeframes == "" {
			trader.Timeframes = "4h"
		}
		traders = append(traders, &trader)
	}

	return traders, nil
}

// UpdateTraderStatus 更新交易员状态
func (d *Database) UpdateTraderStatus(userID, id string, isRunning bool) error {
	collection := d.db.Collection("traders")
	filter := bson.M{"id": id, "user_id": userID}
	update := bson.M{"$set": bson.M{"is_running": isRunning, "updated_at": time.Now()}}
	_, err := collection.UpdateOne(d.ctx, filter, update)
	return err
}

// UpdateTrader 更新交易员配置
func (d *Database) UpdateTrader(trader *TraderRecord) error {
	collection := d.db.Collection("traders")
	filter := bson.M{"id": trader.ID, "user_id": trader.UserID}
	update := bson.M{
		"$set": bson.M{
			"name":                   trader.Name,
			"ai_model_id":            trader.AIModelID,
			"exchange_id":            trader.ExchangeID,
			"scan_interval_minutes":  trader.ScanIntervalMinutes,
			"btc_eth_leverage":       trader.BTCETHLeverage,
			"altcoin_leverage":       trader.AltcoinLeverage,
			"trading_symbols":        trader.TradingSymbols,
			"custom_prompt":          trader.CustomPrompt,
			"override_base_prompt":   trader.OverrideBasePrompt,
			"system_prompt_template": trader.SystemPromptTemplate,
			"is_cross_margin":        trader.IsCrossMargin,
			"taker_fee_rate":         trader.TakerFeeRate,
			"maker_fee_rate":         trader.MakerFeeRate,
			"order_strategy":         trader.OrderStrategy,
			"limit_price_offset":     trader.LimitPriceOffset,
			"limit_timeout_seconds":  trader.LimitTimeoutSeconds,
			"timeframes":             trader.Timeframes,
			"updated_at":             time.Now(),
		},
	}
	_, err := collection.UpdateOne(d.ctx, filter, update)
	return err
}

// UpdateTraderCustomPrompt 更新交易员自定义Prompt
func (d *Database) UpdateTraderCustomPrompt(userID, id string, customPrompt string, overrideBase bool) error {
	collection := d.db.Collection("traders")
	filter := bson.M{"id": id, "user_id": userID}
	update := bson.M{"$set": bson.M{"custom_prompt": customPrompt, "override_base_prompt": overrideBase, "updated_at": time.Now()}}
	_, err := collection.UpdateOne(d.ctx, filter, update)
	return err
}

// UpdateTraderInitialBalance 更新交易员初始余额（仅支持手动更新）
// ⚠️ 注意：系统不会自动调用此方法，仅供用户在充值/提现后手动同步使用
func (d *Database) UpdateTraderInitialBalance(userID, id string, newBalance float64) error {
	collection := d.db.Collection("traders")
	filter := bson.M{"id": id, "user_id": userID}
	update := bson.M{"$set": bson.M{"initial_balance": newBalance, "updated_at": time.Now()}}
	_, err := collection.UpdateOne(d.ctx, filter, update)
	return err
}

// DeleteTrader 删除交易员
func (d *Database) DeleteTrader(userID, id string) error {
	collection := d.db.Collection("traders")
	filter := bson.M{"id": id, "user_id": userID}
	_, err := collection.DeleteOne(d.ctx, filter)
	return err
}

// GetTraderConfig 获取交易员完整配置（包含AI模型和交易所信息）
func (d *Database) GetTraderConfig(userID, traderID string) (*TraderRecord, *AIModelConfig, *ExchangeConfig, error) {
	// 获取交易员
	tradersCollection := d.db.Collection("traders")
	filter := bson.M{"id": traderID, "user_id": userID}
	var trader TraderRecord
	err := tradersCollection.FindOne(d.ctx, filter).Decode(&trader)
	if err != nil {
		return nil, nil, nil, err
	}

	// 设置默认值
	if trader.BTCETHLeverage == 0 {
		trader.BTCETHLeverage = 5
	}
	if trader.AltcoinLeverage == 0 {
		trader.AltcoinLeverage = 5
	}
	if trader.SystemPromptTemplate == "" {
		trader.SystemPromptTemplate = "default"
	}
	if trader.TakerFeeRate == 0 {
		trader.TakerFeeRate = 0.0004
	}
	if trader.MakerFeeRate == 0 {
		trader.MakerFeeRate = 0.0002
	}
	if trader.OrderStrategy == "" {
		trader.OrderStrategy = "conservative_hybrid"
	}
	if trader.LimitPriceOffset == 0 {
		trader.LimitPriceOffset = -0.03
	}
	if trader.LimitTimeoutSeconds == 0 {
		trader.LimitTimeoutSeconds = 60
	}
	if trader.Timeframes == "" {
		trader.Timeframes = "4h"
	}

	// 获取AI模型
	aiModelsCollection := d.db.Collection("ai_models")
	filter = bson.M{"id": trader.AIModelID}
	var aiModel AIModelConfig
	err = aiModelsCollection.FindOne(d.ctx, filter).Decode(&aiModel)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("获取AI模型失败: %w", err)
	}

	// 获取交易所
	exchangesCollection := d.db.Collection("exchanges")
	filter = bson.M{"id": trader.ExchangeID}
	var exchange ExchangeConfig
	err = exchangesCollection.FindOne(d.ctx, filter).Decode(&exchange)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("获取交易所失败: %w", err)
	}

	// 处理空值
	if aiModel.CustomAPIURL == "" {
		aiModel.CustomAPIURL = ""
	}
	if aiModel.CustomModelName == "" {
		aiModel.CustomModelName = ""
	}
	if exchange.HyperliquidWalletAddr == "" {
		exchange.HyperliquidWalletAddr = ""
	}
	if exchange.AsterUser == "" {
		exchange.AsterUser = ""
	}
	if exchange.AsterSigner == "" {
		exchange.AsterSigner = ""
	}

	// 解密敏感数据
	aiModel.APIKey = d.decryptSensitiveData(aiModel.APIKey)
	exchange.APIKey = d.decryptSensitiveData(exchange.APIKey)
	exchange.SecretKey = d.decryptSensitiveData(exchange.SecretKey)
	exchange.AsterPrivateKey = d.decryptSensitiveData(exchange.AsterPrivateKey)

	return &trader, &aiModel, &exchange, nil
}

// GetSystemConfig 获取系统配置
func (d *Database) GetSystemConfig(key string) (string, error) {
	collection := d.db.Collection("system_config")
	filter := bson.M{"key": key}
	var result struct {
		Key   string `bson:"key"`
		Value string `bson:"value"`
	}
	err := collection.FindOne(d.ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return "", nil
		}
		return "", err
	}
	return result.Value, nil
}

// SetSystemConfig 设置系统配置
func (d *Database) SetSystemConfig(key, value string) error {
	collection := d.db.Collection("system_config")
	filter := bson.M{"key": key}
	update := bson.M{
		"$set": bson.M{
			"key":        key,
			"value":      value,
			"updated_at": time.Now(),
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := collection.UpdateOne(d.ctx, filter, update, opts)
	return err
}

// CreateUserSignalSource 创建用户信号源配置
func (d *Database) CreateUserSignalSource(userID, coinPoolURL, oiTopURL string) error {
	collection := d.db.Collection("user_signal_sources")
	filter := bson.M{"user_id": userID}
	update := bson.M{
		"$set": bson.M{
			"user_id":       userID,
			"coin_pool_url": coinPoolURL,
			"oi_top_url":    oiTopURL,
			"updated_at":    time.Now(),
		},
		"$setOnInsert": bson.M{
			"created_at": time.Now(),
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := collection.UpdateOne(d.ctx, filter, update, opts)
	return err
}

// GetUserSignalSource 获取用户信号源配置
func (d *Database) GetUserSignalSource(userID string) (*UserSignalSource, error) {
	collection := d.db.Collection("user_signal_sources")
	filter := bson.M{"user_id": userID}
	var source UserSignalSource
	err := collection.FindOne(d.ctx, filter).Decode(&source)
	if err != nil {
		return nil, err
	}
	return &source, nil
}

// UpdateUserSignalSource 更新用户信号源配置
func (d *Database) UpdateUserSignalSource(userID, coinPoolURL, oiTopURL string) error {
	collection := d.db.Collection("user_signal_sources")
	filter := bson.M{"user_id": userID}
	update := bson.M{
		"$set": bson.M{
			"coin_pool_url": coinPoolURL,
			"oi_top_url":    oiTopURL,
			"updated_at":    time.Now(),
		},
	}
	_, err := collection.UpdateOne(d.ctx, filter, update)
	return err
}

// GetCustomCoins 获取所有交易员自定义币种 / Get all trader-customized currencies
func (d *Database) GetCustomCoins() []string {
	collection := d.db.Collection("traders")
	filter := bson.M{"custom_coins": bson.M{"$ne": ""}}
	cursor, err := collection.Find(d.ctx, filter, options.Find().SetProjection(bson.M{"custom_coins": 1}))
	if err != nil {
		log.Printf("⚠️  查询custom_coins失败: %v", err)
		return []string{}
	}
	defer cursor.Close(d.ctx)

	var symbols []string
	var allCoins []string
	for cursor.Next(d.ctx) {
		var trader struct {
			CustomCoins string `bson:"custom_coins"`
		}
		if err := cursor.Decode(&trader); err != nil {
			continue
		}
		if trader.CustomCoins != "" {
			// 解析 JSON 格式的币种列表
			var coins []string
			if err := json.Unmarshal([]byte(trader.CustomCoins), &coins); err == nil {
				allCoins = append(allCoins, coins...)
			}
		}
	}

	// 检测用户是否未配置币种 - 兼容性
	if len(allCoins) == 0 {
		symbolJSON, _ := d.GetSystemConfig("default_coins")
		if err := json.Unmarshal([]byte(symbolJSON), &symbols); err != nil {
			log.Printf("⚠️  解析default_coins配置失败: %v，使用硬编码默认值", err)
			symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}
		}
	} else {
		// filter Symbol
		for _, s := range allCoins {
			if s == "" {
				continue
			}
			coin := market.Normalize(s)
			if !slices.Contains(symbols, coin) {
				symbols = append(symbols, coin)
			}
		}
	}
	return symbols
}

// GetAllTimeframes 获取所有交易员配置的时间线并集 / Get union of all trader timeframes
func (d *Database) GetAllTimeframes() []string {
	collection := d.db.Collection("traders")
	filter := bson.M{
		"timeframes": bson.M{"$ne": ""},
		"is_running": true,
	}
	cursor, err := collection.Find(d.ctx, filter, options.Find().SetProjection(bson.M{"timeframes": 1}))
	if err != nil {
		log.Printf("查询 trader timeframes 失败: %v", err)
		return []string{"4h"} // 默认返回 4h
	}
	defer cursor.Close(d.ctx)

	timeframeSet := make(map[string]bool)
	for cursor.Next(d.ctx) {
		var trader struct {
			Timeframes string `bson:"timeframes"`
		}
		if err := cursor.Decode(&trader); err != nil {
			continue
		}
		// 解析逗号分隔的时间线
		for _, tf := range strings.Split(trader.Timeframes, ",") {
			tf = strings.TrimSpace(tf)
			if tf != "" {
				timeframeSet[tf] = true
			}
		}
	}

	// 转换为切片
	result := make([]string, 0, len(timeframeSet))
	for tf := range timeframeSet {
		result = append(result, tf)
	}

	// 如果没有配置，返回默认值
	if len(result) == 0 {
		return []string{"15m", "1h", "4h"}
	}

	log.Printf("📊 从数据库加载所有活跃 trader 的时间线: %v", result)
	return result
}

// Close 关闭数据库连接
func (d *Database) Close() error {
	return d.client.Disconnect(d.ctx)
}

// LoadBetaCodesFromFile 从文件加载内测码到数据库
func (d *Database) LoadBetaCodesFromFile(filePath string) error {
	// 读取文件内容
	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("读取内测码文件失败: %w", err)
	}

	// 按行分割内测码
	lines := strings.Split(string(content), "\n")
	var codes []string
	for _, line := range lines {
		code := strings.TrimSpace(line)
		if code != "" && !strings.HasPrefix(code, "#") {
			codes = append(codes, code)
		}
	}

	// 批量插入内测码
	collection := d.db.Collection("beta_codes")
	insertedCount := 0
	for _, code := range codes {
		filter := bson.M{"code": code}
		update := bson.M{
			"$setOnInsert": bson.M{
				"code":       code,
				"used":       false,
				"used_by":    "",
				"used_at":    nil,
				"created_at": time.Now(),
			},
		}
		opts := options.Update().SetUpsert(true)
		result, err := collection.UpdateOne(d.ctx, filter, update, opts)
		if err != nil {
			log.Printf("插入内测码 %s 失败: %v", code, err)
			continue
		}

		if result.UpsertedCount > 0 {
			insertedCount++
		}
	}

	log.Printf("✅ 成功加载 %d 个内测码到数据库 (总计 %d 个)", insertedCount, len(codes))
	return nil
}

// ValidateBetaCode 验证内测码是否有效且未使用
func (d *Database) ValidateBetaCode(code string) (bool, error) {
	collection := d.db.Collection("beta_codes")
	filter := bson.M{"code": code}
	var result struct {
		Used bool `bson:"used"`
	}
	err := collection.FindOne(d.ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil // 内测码不存在
		}
		return false, err
	}
	return !result.Used, nil // 内测码存在且未使用
}

// UseBetaCode 使用内测码（标记为已使用）
func (d *Database) UseBetaCode(code, userEmail string) error {
	collection := d.db.Collection("beta_codes")
	filter := bson.M{"code": code, "used": false}
	update := bson.M{
		"$set": bson.M{
			"used":    true,
			"used_by": userEmail,
			"used_at": time.Now(),
		},
	}
	result, err := collection.UpdateOne(d.ctx, filter, update)
	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("内测码无效或已被使用")
	}

	return nil
}

// GetBetaCodeStats 获取内测码统计信息
func (d *Database) GetBetaCodeStats() (total, used int, err error) {
	collection := d.db.Collection("beta_codes")

	total64, err := collection.CountDocuments(d.ctx, bson.M{})
	if err != nil {
		return 0, 0, err
	}
	total = int(total64)

	used64, err := collection.CountDocuments(d.ctx, bson.M{"used": true})
	if err != nil {
		return 0, 0, err
	}
	used = int(used64)

	return total, used, nil
}

// SetCryptoService 设置加密服务
func (d *Database) SetCryptoService(cs *crypto.CryptoService) {
	d.cryptoService = cs
}

// encryptSensitiveData 加密敏感数据用于存储
func (d *Database) encryptSensitiveData(plaintext string) string {
	if d.cryptoService == nil || plaintext == "" {
		return plaintext
	}

	encrypted, err := d.cryptoService.EncryptForStorage(plaintext)
	if err != nil {
		log.Printf("⚠️ 加密失败: %v", err)
		return plaintext // 返回明文作为降级处理
	}

	return encrypted
}

// decryptSensitiveData 解密敏感数据
func (d *Database) decryptSensitiveData(encrypted string) string {
	if d.cryptoService == nil || encrypted == "" {
		return encrypted
	}

	// 如果不是加密格式，直接返回
	if !d.cryptoService.IsEncryptedStorageValue(encrypted) {
		return encrypted
	}

	decrypted, err := d.cryptoService.DecryptFromStorage(encrypted)
	if err != nil {
		log.Printf("⚠️ 解密失败: %v", err)
		return encrypted // 返回加密文本作为降级处理
	}

	return decrypted
}

// cleanupLegacyColumns MongoDB 不需要此函数，保留接口兼容性
// This function is not needed for MongoDB as it doesn't have columns
func (d *Database) cleanupLegacyColumns() error {
	// MongoDB 不需要清理遗留列，直接返回
	return nil
}

// SaveDecisionLog 保存决策日志到MongoDB
func (d *Database) SaveDecisionLog(userID, traderID string, record interface{}) error {
	collection := d.db.Collection("decision_logs")
	doc := bson.M{
		"user_id":    userID,
		"trader_id":  traderID,
		"record":     record,
		"created_at": time.Now(),
	}
	_, err := collection.InsertOne(d.ctx, doc)
	return err
}

// GetDecisionLogs 从MongoDB获取决策日志
func (d *Database) GetDecisionLogs(userID, traderID string, limit int) ([]bson.M, error) {
	collection := d.db.Collection("decision_logs")
	filter := bson.M{
		"user_id":   userID,
		"trader_id": traderID,
	}
	opts := options.Find().SetSort(bson.M{"created_at": -1}).SetLimit(int64(limit))
	cursor, err := collection.Find(d.ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(d.ctx)

	var results []bson.M
	for cursor.Next(d.ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		// 返回record字段，确保类型为bson.M
		if record, ok := doc["record"].(bson.M); ok {
			results = append(results, record)
		}
	}

	// 反转数组，让时间从旧到新排列
	for i, j := 0, len(results)-1; i < j; i, j = i+1, j-1 {
		results[i], results[j] = results[j], results[i]
	}

	return results, nil
}
