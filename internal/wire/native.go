package wire

import "encoding/json"

type NativeEvent struct {
	Type         string `json:"type"`
	ToolCallID   string `json:"toolCallId,omitempty"`
	ToolName     string `json:"toolName,omitempty"`
	StopReason   string `json:"stopReason,omitempty"`
	WillContinue *bool  `json:"willContinue,omitempty"`
	Outcome      string `json:"outcome,omitempty"`
}
type NativeAsyncJobs struct {
	Running float64 `json:"running"`
	Pending float64 `json:"pending"`
}
type NativeSnapshot struct {
	Harness         string           `json:"harness"`
	NativeSessionID string           `json:"nativeSessionId"`
	NativeAgentID   string           `json:"nativeAgentId,omitempty"`
	CWD             string           `json:"cwd"`
	Agent           *SessionAgent    `json:"agent,omitempty"`
	Model           *SessionModel    `json:"model,omitempty"`
	Parent          *SessionRef      `json:"parent,omitempty"`
	HarnessVersion  string           `json:"harnessVersion,omitempty"`
	IsIdle          bool             `json:"isIdle"`
	Mode            string           `json:"mode,omitempty"`
	HasUI           *bool            `json:"hasUI,omitempty"`
	Event           *NativeEvent     `json:"event,omitempty"`
	AsyncJobs       *NativeAsyncJobs `json:"asyncJobs,omitempty"`
}
type NativeNotice struct {
	State     string `json:"state"`
	Operation string `json:"operation"`
	Code      string `json:"code,omitempty"`
	Status    *int   `json:"status,omitempty"`
}
type NativeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type NativeMessage struct {
	Version  int                  `json:"version"`
	ID       string               `json:"id,omitempty"`
	Method   string               `json:"method,omitempty"`
	Snapshot *NativeSnapshot      `json:"snapshot,omitempty"`
	Ref      *SessionRef          `json:"ref,omitempty"`
	Input    *SessionListInput    `json:"input,omitempty"`
	Result   json.RawMessage      `json:"result,omitempty"`
	Error    *NativeError         `json:"error,omitempty"`
	Notice   *NativeNotice        `json:"notice,omitempty"`
	Status   *ProgramStatus       `json:"status,omitempty"`
	Message  *Message             `json:"message,omitempty"`
	Send     *MessageSendParams   `json:"send,omitempty"`
	Reply    *MessageReplyParams  `json:"reply,omitempty"`
	Get      *MessageGetInput     `json:"get,omitempty"`
	Ack      *ConnectorMessageAck `json:"ack,omitempty"`
}
