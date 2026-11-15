package wire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOAuthConfigWireValidation(t *testing.T) {
	valid := InspectorConfig{Host: "127.0.0.1", Principals: []InspectorPrincipal{{ID: "oauth", Owner: "owner"}}, OAuth: &InspectorOAuthConfig{Resource: "https://coordinator.example/mcp", Issuer: "https://issuer.example", JWKSURL: "https://issuer.example/keys", Bindings: []InspectorOAuthBinding{{Subject: "alice", PrincipalID: "oauth"}}}}
	b, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "tokenHash") {
		t.Fatalf("OAuth principal emitted token hash: %s", b)
	}
	if _, err = Decode[InspectorConfig](strings.NewReader(string(b)), int64(len(b))); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*InspectorOAuthConfig)
	}{
		{"non HTTPS issuer", func(c *InspectorOAuthConfig) { c.Issuer = "http://issuer.example" }},
		{"non HTTPS jwks", func(c *InspectorOAuthConfig) { c.JWKSURL = "http://issuer.example/keys" }},
		{"resource path", func(c *InspectorOAuthConfig) { c.Resource = "https://coordinator.example/other" }},
		{"userinfo", func(c *InspectorOAuthConfig) { c.JWKSURL = "https://user@issuer.example/keys" }},
		{"query", func(c *InspectorOAuthConfig) { c.Issuer = "https://issuer.example?x=y" }},
		{"empty bindings", func(c *InspectorOAuthConfig) { c.Bindings = []InspectorOAuthBinding{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := *valid.OAuth
			tc.mutate(&c)
			cfg := valid
			cfg.OAuth = &c
			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = Decode[InspectorConfig](strings.NewReader(string(data)), int64(len(data))); err == nil {
				t.Fatalf("accepted %s", data)
			}
		})
	}
	loopback := *valid.OAuth
	loopback.Resource = "http://127.0.0.1:8080/mcp"
	loopback.Issuer = "http://localhost:8081"
	loopback.JWKSURL = "http://[::1]:8081/keys"
	valid.OAuth = &loopback
	b, err = json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Decode[InspectorConfig](strings.NewReader(string(b)), int64(len(b))); err != nil {
		t.Fatalf("loopback URLs: %v", err)
	}
}
