package api

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type WebhookContent struct {
	TraderID string  // 交易员ID
	Symbol   string  // 交易对
	Interval string  // 时间间隔
	Time     string  // 时间
	Type     string  // 类型
	Open     float64 // 开盘价
	Close    float64 // 收盘价
	High     float64 // 最高价
	Low      float64 // 最低价
	Volume   float64 // 成交量
	Content  string  // 内容
}

func (s *Server) handleWebhook(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	text := strings.TrimSpace(string(body))
	if text == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "empty payload"})
		return
	}

	hookContent := WebhookContent{}

	// 示例格式: traderID BTCUSDT 15m 2025-11-14T07:37:00Z type more content...
	for i, field := range strings.Fields(text) {
		switch i {
		case 0:
			hookContent.TraderID = field
		case 1:
			hookContent.Symbol = field
		case 2:
			hookContent.Interval = field
		case 3:
			hookContent.Time = field
		case 4:
			hookContent.Type = field
		case 5:
			hookContent.Open, err = strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("解析 Open 失败: %v, field: %s", err, field)
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid open price: " + field})
				return
			}
		case 6:
			hookContent.Close, err = strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("解析 Close 失败: %v, field: %s", err, field)
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid close price: " + field})
				return
			}
		case 7:
			hookContent.High, err = strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("解析 High 失败: %v, field: %s", err, field)
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid high price: " + field})
				return
			}
		case 8:
			hookContent.Low, err = strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("解析 Low 失败: %v, field: %s", err, field)
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid low price: " + field})
				return
			}
		case 9:
			hookContent.Volume, err = strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("解析 Volume 失败: %v, field: %s", err, field)
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid volume: " + field})
				return
			}
		default:
			hookContent.Content += field
		}
	}

	log.Printf("Hook Content: TraderID=%s, Symbol=%s, Interval=%s, Time=%s, Type=%s, Open=%f, Close=%f, High=%f, Low=%f, Volume=%f, Content=%s",
		hookContent.TraderID, hookContent.Symbol, hookContent.Interval, hookContent.Time, hookContent.Type, hookContent.Open, hookContent.Close, hookContent.High, hookContent.Low, hookContent.Volume, hookContent.Content)

	if hookContent.Type == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "type is required"})
		return
	}

	promptTemp := os.Getenv("TYPE_" + hookContent.Type)
	if promptTemp == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "prompt template not found for type: " + hookContent.Type})
		return
	}

	log.Printf("原始 promptTemp: %s", promptTemp)

	// 替换 promptTemp 中的 ${Symbol} 为 hookContent.Symbol
	promptTemp = strings.ReplaceAll(promptTemp, "${Symbol}", hookContent.Symbol)
	// 替换 promptTemp 中的 ${Interval} 为 hookContent.Interval
	promptTemp = strings.ReplaceAll(promptTemp, "${Interval}", hookContent.Interval)
	// 替换 promptTemp 中的 ${Time} 为 hookContent.Time
	promptTemp = strings.ReplaceAll(promptTemp, "${Time}", hookContent.Time)
	// 替换 promptTemp 中的 ${Open} 为 hookContent.Open
	promptTemp = strings.ReplaceAll(promptTemp, "${Open}", fmt.Sprintf("%.6f", hookContent.Open))
	// 替换 promptTemp 中的 ${Close} 为 hookContent.Close
	promptTemp = strings.ReplaceAll(promptTemp, "${Close}", fmt.Sprintf("%.6f", hookContent.Close))
	// 替换 promptTemp 中的 ${High} 为 hookContent.High
	promptTemp = strings.ReplaceAll(promptTemp, "${High}", fmt.Sprintf("%.6f", hookContent.High))
	// 替换 promptTemp 中的 ${Low} 为 hookContent.Low
	promptTemp = strings.ReplaceAll(promptTemp, "${Low}", fmt.Sprintf("%.6f", hookContent.Low))
	// 替换 promptTemp 中的 ${Volume} 为 hookContent.Volume
	promptTemp = strings.ReplaceAll(promptTemp, "${Volume}", fmt.Sprintf("%.6f", hookContent.Volume))

	log.Printf("替换后 promptTemp: %s", promptTemp)

	traderID := hookContent.TraderID

	autoTrader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	if err := autoTrader.RunCycle(promptTemp); err != nil {
		log.Printf("❌ Webhook 触发 RunCycle 失败 [%s]: %v", traderID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Webhook received",
	})
}
