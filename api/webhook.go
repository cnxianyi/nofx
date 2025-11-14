package api

import (
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func Webhook(c *gin.Context) {
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

	log.Printf("Webhook received: %s", text)

	hookContent := make([]string, 5)

	// 示例格式: BTCUSDT 15m 2025-11-14T07:37:00Z type more content...
	for i, field := range strings.Fields(text) {
		if i < 4 {
			hookContent[i] = field
			continue
		}
		if hookContent[4] == "" {
			hookContent[4] = field
		} else {
			hookContent[4] += " " + field
		}
	}

	log.Printf("Hook Content: %s", hookContent[4])

	c.JSON(http.StatusOK, gin.H{"message": "Webhook received"})
}
