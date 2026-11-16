package coordinator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const hostedLimit = 128
const hostedBodyLimit = 65536

type hostedSubscription struct {
	ID, Generation, Owner, PrincipalID, Name, URL, Secret string
	PreviousSecret                                        string
	PreviousSecretUntil                                   time.Time
	ExpiresAt                                             time.Time
}

type hostedStore struct {
	mu            sync.Mutex
	directory     string
	subscriptions map[string]hostedSubscription
}

func validHostedSubscription(s hostedSubscription) bool {
	if len(s.ID) == 0 || len(s.ID) > 128 || strings.ContainsAny(s.ID, "\r\n") || len(s.Generation) == 0 || len(s.Generation) > 128 || strings.ContainsAny(s.Generation, "\r\n") || len(s.Owner) == 0 || len(s.Owner) > 256 || len(s.PrincipalID) == 0 || len(s.PrincipalID) > 256 || len(s.Name) == 0 || len(s.Name) > 256 || len(s.URL) > 2048 || s.ExpiresAt.IsZero() {
		return false
	}
	if _, err := hostedKey(s.Secret); err != nil {
		return false
	}
	if (s.PreviousSecret == "") != s.PreviousSecretUntil.IsZero() || s.PreviousSecretUntil.After(s.ExpiresAt) {
		return false
	}
	if s.PreviousSecret != "" {
		if _, err := hostedKey(s.PreviousSecret); err != nil {
			return false
		}
	}
	return validHostedURL(s.URL) != nil
}

func openHostedStore(directory string) (*hostedStore, error) {
	if err := WritePrivate(directory, map[string][]byte{}); err != nil {
		return nil, err
	}
	if err := checkDir(directory, true); err != nil {
		return nil, err
	}
	s := &hostedStore{directory: directory, subscriptions: make(map[string]hostedSubscription)}
	b, err := readPrivate(filepath.Join(directory, "subscriptions.json"), true)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	var records []hostedSubscription
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&records); err != nil || records == nil || len(records) > hostedLimit {
		return nil, errors.New("invalid hosted state")
	}
	if err = dec.Decode(new(any)); err != io.EOF {
		return nil, errors.New("invalid hosted state")
	}
	seen := make(map[string]struct{}, len(records))
	for _, sub := range records {
		if !validHostedSubscription(sub) {
			return nil, errors.New("invalid hosted state")
		}
		if _, exists := seen[sub.ID]; exists {
			return nil, errors.New("duplicate hosted subscription")
		}
		seen[sub.ID] = struct{}{}
		if sub.ExpiresAt.After(time.Now()) {
			s.subscriptions[sub.ID] = sub
		}
	}
	return s, nil
}

// ponytail: one process owns this store; use an interprocess lock if multiple writers are needed.
func (s *hostedStore) save(records map[string]hostedSubscription) (bool, error) {
	if err := checkDir(s.directory, true); err != nil {
		return false, err
	}
	path := filepath.Join(s.directory, "subscriptions.json")
	if _, err := readPrivate(path, true); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	list := make([]hostedSubscription, 0, len(records))
	for _, sub := range records {
		list = append(list, sub)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	b, err := json.Marshal(list)
	if err != nil {
		return false, err
	}
	if len(b) > maxPrivateBytes {
		return false, errors.New("hosted state exceeds bound")
	}
	f, err := os.CreateTemp(s.directory, ".subscriptions-")
	if err != nil {
		return false, err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return false, err
	}
	dir, err := os.Open(s.directory)
	if err != nil {
		return true, err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return true, err
}

func (s *hostedStore) put(sub hostedSubscription) error {
	if !validHostedSubscription(sub) || !sub.ExpiresAt.After(time.Now()) {
		return errors.New("invalid hosted subscription")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]hostedSubscription, len(s.subscriptions)+1)
	for id, existing := range s.subscriptions {
		if existing.ExpiresAt.After(time.Now()) {
			next[id] = existing
		}
	}
	if existing, ok := next[sub.ID]; ok && (existing.Generation != sub.Generation || existing.Owner != sub.Owner || existing.PrincipalID != sub.PrincipalID || existing.URL != sub.URL || existing.Name != sub.Name) {
		return errors.New("hosted subscription identity mismatch")
	}
	next[sub.ID] = sub
	if len(next) > hostedLimit {
		return errors.New("hosted subscription quota exceeded")
	}
	committed, err := s.save(next)
	if committed {
		s.subscriptions = next
	}
	return err
}
func (s *hostedStore) remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]hostedSubscription, len(s.subscriptions))
	for key, sub := range s.subscriptions {
		if key != id && sub.ExpiresAt.After(time.Now()) {
			next[key] = sub
		}
	}
	committed, err := s.save(next)
	if committed {
		s.subscriptions = next
	}
	return err
}
func (s *hostedStore) get(id string) (hostedSubscription, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkDir(s.directory, true); err != nil {
		return hostedSubscription{}, false, err
	}
	sub, ok := s.subscriptions[id]
	return sub, ok && sub.ExpiresAt.After(time.Now()), nil
}
func (s *hostedStore) list() ([]hostedSubscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkDir(s.directory, true); err != nil {
		return nil, err
	}
	result := make([]hostedSubscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		if sub.ExpiresAt.After(time.Now()) {
			result = append(result, sub)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

type hostedDelivery struct {
	hosts  map[string]struct{}
	client *http.Client
}

func validHostname(host string) bool {
	if host == "" || len(host) > 253 || host != strings.ToLower(host) || net.ParseIP(host) != nil {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}
func validHostedURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || !validHostname(u.Hostname()) || (u.Port() != "" && u.Port() != "443") {
		return nil
	}
	return u
}
func hostedKey(secret string) ([]byte, error) {
	if !strings.HasPrefix(secret, "whsec_") {
		return nil, errors.New("invalid webhook secret")
	}
	key, err := base64.StdEncoding.Strict().DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil || len(key) < 24 || len(key) > 64 {
		return nil, errors.New("invalid webhook secret")
	}
	return key, nil
}

var hostedBlockedNetworks = func() []netip.Prefix {
	values := []string{"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "::/128", "::1/128", "::ffff:0:0/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20", "fc00::/7", "fe80::/10", "ff00::/8"}
	result := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		result = append(result, netip.MustParsePrefix(value))
	}
	return result
}()

func hostedPublicIP(ip netip.Addr) bool {
	if !ip.IsValid() || ip.Is4In6() || !ip.IsGlobalUnicast() || (ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip)) {
		return false
	}
	for _, prefix := range hostedBlockedNetworks {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
func newHostedDelivery(callbackHosts []string) (*hostedDelivery, error) {
	d := &hostedDelivery{hosts: make(map[string]struct{}, len(callbackHosts))}
	for _, host := range callbackHosts {
		if !validHostname(host) {
			return nil, errors.New("invalid callback host")
		}
		d.hosts[host] = struct{}{}
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || port != "443" {
			return nil, errors.New("invalid callback destination")
		}
		if _, ok := d.hosts[host]; !ok {
			return nil, errors.New("callback host not allowed")
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil || len(ips) == 0 {
			return nil, errors.New("callback DNS failed")
		}
		for _, ip := range ips {
			if !hostedPublicIP(ip) {
				return nil, errors.New("unsafe callback address")
			}
		}
		var dialer net.Dialer
		for _, ip := range ips {
			conn, dialErr := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
			if dialErr == nil {
				return conn, nil
			}
		}
		return nil, errors.New("callback connection failed")
	}
	d.client = &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return d, nil
}
func (d *hostedDelivery) send(ctx context.Context, sub hostedSubscription, id string, body []byte) ([]byte, int, error) {
	u := validHostedURL(sub.URL)
	key, err := hostedKey(sub.Secret)
	now := time.Now()
	if u == nil || err != nil || !validHostedSubscription(sub) || len(id) == 0 || len(id) > 256 || strings.ContainsAny(id, ".\r\n") || len(body) > hostedBodyLimit || !sub.ExpiresAt.After(now) {
		return nil, 0, errors.New("invalid callback request")
	}
	if _, ok := d.hosts[u.Hostname()]; !ok {
		return nil, 0, errors.New("callback host not allowed")
	}
	timestamp := strconv.FormatInt(now.Unix(), 10)
	mac := hmac.New(sha256.New, key)
	fmt.Fprintf(mac, "%s.%s.", id, timestamp)
	mac.Write(body)
	signature := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if sub.PreviousSecret != "" && sub.PreviousSecretUntil.After(now) {
		previousKey, err := hostedKey(sub.PreviousSecret)
		if err != nil {
			return nil, 0, errors.New("invalid callback request")
		}
		mac = hmac.New(sha256.New, previousKey)
		fmt.Fprintf(mac, "%s.%s.", id, timestamp)
		mac.Write(body)
		signature += " v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL, bytes.NewReader(body))
	if err != nil {
		return nil, 0, errors.New("invalid callback request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-MCP-Subscription-Id", sub.ID)
	req.Header.Set("webhook-id", id)
	req.Header.Set("webhook-timestamp", timestamp)
	req.Header.Set("webhook-signature", signature)
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, 0, errors.New("callback request failed")
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, hostedBodyLimit+1))
	if err != nil || len(response) > hostedBodyLimit {
		return nil, resp.StatusCode, errors.New("invalid callback response")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, resp.StatusCode, errors.New("callback rejected")
	}
	return response, resp.StatusCode, nil
}
func (d *hostedDelivery) verify(ctx context.Context, sub hostedSubscription) error {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errors.New("challenge generation failed")
	}
	challenge := base64.RawURLEncoding.EncodeToString(nonce[:])
	body, err := json.Marshal(struct {
		Type      string `json:"type"`
		Challenge string `json:"challenge"`
	}{"verification", challenge})
	if err != nil {
		return errors.New("challenge generation failed")
	}
	response, _, err := d.send(ctx, sub, "verify_"+challenge, body)
	if err != nil {
		return err
	}
	var echoed struct {
		Challenge string `json:"challenge"`
	}
	dec := json.NewDecoder(bytes.NewReader(response))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&echoed); err != nil {
		return errors.New("invalid callback challenge")
	}
	if err := dec.Decode(new(any)); err != io.EOF || subtle.ConstantTimeCompare([]byte(echoed.Challenge), []byte(challenge)) != 1 {
		return errors.New("invalid callback challenge")
	}
	return nil
}
