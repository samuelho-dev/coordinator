package coordinator

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"github.com/samuelho-dev/coordinator/internal/wire"
)

const readScope = "coordinator:read"
const messageScope = "coordinator:message"
const resourceMetadataPath = "/.well-known/oauth-protected-resource"

type oauthAccess struct {
	metadataURL string
	bindings    map[string]wire.InspectorPrincipal
	keys        keyfunc.Keyfunc
	parser      *jwt.Parser
	metadata    http.Handler
	cancel      context.CancelFunc
}

type accessClaims struct {
	jwt.RegisteredClaims
	Scope *string  `json:"scope"`
	SCP   []string `json:"scp"`
}

// Metadata and key URLs come only from operator configuration, never token headers.
type jwksTransport struct{}

func (jwksTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.ContentLength > 1<<20 {
		_ = resp.Body.Close()
		return nil, errors.New("OAuth signing key response exceeds bound")
	}
	resp.Body = http.MaxBytesReader(nil, resp.Body, 1<<20)
	return resp, nil
}

func newOAuth(cfg *wire.InspectorOAuthConfig, principals map[string]wire.InspectorPrincipal) (*oauthAccess, error) {
	if err := wire.Validate(*cfg); err != nil {
		return nil, errors.New("Invalid OAuth configuration")
	}
	bindings := make(map[string]wire.InspectorPrincipal, len(cfg.Bindings))
	bound := make(map[string]bool, len(cfg.Bindings))
	for _, b := range cfg.Bindings {
		p, ok := principals[b.PrincipalID]
		if !ok || p.TokenHash != "" || b.Subject == "" || bound[p.ID] {
			return nil, errors.New("OAuth binding requires a dedicated token-free principal")
		}
		if _, exists := bindings[b.Subject]; exists {
			return nil, errors.New("Ambiguous OAuth subject")
		}
		bindings[b.Subject], bound[p.ID] = p, true
	}
	for _, p := range principals {
		if p.TokenHash == "" && !bound[p.ID] {
			return nil, errors.New("OAuth principal requires a subject binding")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	failFirst := false
	keys, err := keyfunc.NewDefaultOverrideCtx(ctx, []string{cfg.JWKSURL}, keyfunc.Override{
		Client: &http.Client{Timeout: 10 * time.Second, Transport: jwksTransport{}, CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("OAuth signing key redirects prohibited")
		}},
		HTTPTimeout: 10 * time.Second, NoErrorReturnFirstHTTPReq: &failFirst,
		RefreshInterval: 5 * time.Minute, RateLimitWaitMax: time.Second,
	})
	if err != nil {
		cancel()
		return nil, errors.New("OAuth signing keys unavailable")
	}
	u, _ := url.Parse(cfg.Resource) // Validated above; no request Host or forwarded headers.
	u.Path = resourceMetadataPath + "/mcp"
	metadataURL := u.String()
	return &oauthAccess{
		metadataURL: metadataURL, bindings: bindings, keys: keys, cancel: cancel,
		parser: jwt.NewParser(jwt.WithValidMethods([]string{"RS256", "ES256"}), jwt.WithIssuer(cfg.Issuer), jwt.WithAudience(cfg.Resource), jwt.WithExpirationRequired(), jwt.WithIssuedAt()),
		metadata: auth.ProtectedResourceMetadataHandler(&oauthex.ProtectedResourceMetadata{
			Resource: cfg.Resource, AuthorizationServers: []string{cfg.Issuer}, ScopesSupported: []string{readScope, messageScope}, BearerMethodsSupported: []string{"header"}, ResourceName: "Coordinator",
		}),
	}, nil
}

func (r *Registry) verifyMCPToken(ctx context.Context, raw string, _ *http.Request) (*auth.TokenInfo, error) {
	if len(raw) > 16*1024 {
		return nil, auth.ErrInvalidToken
	}
	// Static credentials retain their existing authority and never require the IdP.
	if len(raw) <= 505 {
		if p, err := r.Authorize(raw); err == nil {
			return &auth.TokenInfo{UserID: p.ID, Scopes: []string{readScope, messageScope}, Extra: map[string]any{"principal": p, "oauth": false}}, nil
		}
	}
	if r.oauth == nil {
		return nil, auth.ErrInvalidToken
	}
	claims := &accessClaims{}
	token, err := r.oauth.parser.ParseWithClaims(raw, claims, r.oauth.keys.KeyfuncCtx(ctx))
	if err != nil || !token.Valid || (token.Header["typ"] != "at+jwt" && token.Header["typ"] != "application/at+jwt") {
		return nil, auth.ErrInvalidToken
	}
	scopes := claims.SCP
	if claims.Scope != nil {
		if scopes != nil {
			return nil, auth.ErrInvalidToken
		}
		scopes = strings.Split(*claims.Scope, " ")
	}
	for _, scope := range scopes {
		if scope == "" || strings.IndexFunc(scope, func(r rune) bool { return r < 0x21 || r > 0x7e || r == '"' || r == '\\' }) >= 0 {
			return nil, auth.ErrInvalidToken
		}
	}
	p, ok := r.oauth.bindings[claims.Subject]
	if !ok {
		return nil, auth.ErrInvalidToken
	}
	return &auth.TokenInfo{UserID: p.ID, Scopes: scopes, Expiration: claims.ExpiresAt.Time, Extra: map[string]any{"principal": p, "oauth": true}}, nil
}

func principalForMCP(req *mcp.RequestExtra, messaging bool) (wire.InspectorPrincipal, error) {
	if req == nil || req.TokenInfo == nil {
		return wire.InspectorPrincipal{}, failure(401, "Unauthorized")
	}
	p, ok := req.TokenInfo.Extra["principal"].(wire.InspectorPrincipal)
	if !ok {
		return wire.InspectorPrincipal{}, failure(401, "Unauthorized")
	}
	if messaging && !slices.Contains(req.TokenInfo.Scopes, messageScope) {
		return wire.InspectorPrincipal{}, failure(403, "Messaging permission required")
	}
	return p, nil
}

func oauthCaller(info *auth.TokenInfo) bool {
	return info != nil && info.Extra["oauth"] == true
}
