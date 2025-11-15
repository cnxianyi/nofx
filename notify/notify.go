package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

type NotifyRequest struct {
	Title   string                 `json:"title"`
	Message string                 `json:"message"`
	Target  string                 `json:"target"`
	Extra   map[string]interface{} `json:"extra"`
}

func SendNotify(title string, message string) error {
	url := os.Getenv("NOTIFY_URL")
	if url == "" {
		return fmt.Errorf("NOTIFY_URL is not set")
	}

	target := os.Getenv("TG_TARGET_ID")
	if target == "" {
		return fmt.Errorf("TG_TARGET_ID is not set")
	}

	// 构建请求体
	reqBody := NotifyRequest{
		Title:   title,
		Message: message,
		Target:  target,
		Extra: map[string]interface{}{
			"parse_mode":               "Markdown",
			"disable_web_page_preview": true,
		},
	}

	// 序列化为 JSON
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("序列化请求体失败: %w", err)
	}

	// 创建 HTTP 请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("创建请求失败: %w", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("请求失败，状态码: %d, 响应: %s", resp.StatusCode, resp.Body)
	}

	return nil
}
