package notify

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
)

type NotifyRequest struct {
	Title   string                 `json:"title"`
	Message string                 `json:"message"`
	Target  string                 `json:"target"`
	Extra   map[string]interface{} `json:"extra"`
}

func SendNotify(title string, message string) {
	url := os.Getenv("NOTIFY_URL")
	if url == "" {
		return
	}

	target := os.Getenv("TG_TARGET_ID")
	if target == "" {
		return
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
		return
	}

	// 创建 HTTP 请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		return
	}

}
