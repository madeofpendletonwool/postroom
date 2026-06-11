package main

import (
	"bytes"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/http"
	"net/mail"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gosmtp "github.com/emersion/go-smtp"
	"github.com/gorilla/websocket"
)

//go:embed web/index.html
var indexHTML []byte

//go:embed logo.png
var logoBytes []byte

// ── Types ─────────────────────────────────────────────────────────────────────

type Attachment struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int    `json:"size"`
	Inline      bool   `json:"inline,omitempty"`
	ContentID   string `json:"content_id,omitempty"`
	Data        []byte `json:"-"`
}

type Email struct {
	ID          string        `json:"id"`
	From        string        `json:"from"`
	To          []string      `json:"to"`
	Subject     string        `json:"subject"`
	Text        string        `json:"text"`
	HTML        string        `json:"html"`
	Attachments []*Attachment `json:"attachments"`
	Timestamp   time.Time     `json:"timestamp"`
	Size        int           `json:"size"`
}

// ── Store ─────────────────────────────────────────────────────────────────────

type Store struct {
	mu     sync.RWMutex
	emails []*Email
	max    int
}

func NewStore() *Store { return &Store{max: 500} }

func (s *Store) Add(e *Email) {
	s.mu.Lock()
	s.emails = append([]*Email{e}, s.emails...)
	if len(s.emails) > s.max {
		s.emails = s.emails[:s.max]
	}
	s.mu.Unlock()
}

func (s *Store) All() []*Email {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Email, len(s.emails))
	copy(out, s.emails)
	return out
}

func (s *Store) Get(id string) *Email {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.emails {
		if e.ID == id {
			return e
		}
	}
	return nil
}

func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range s.emails {
		if e.ID == id {
			s.emails = append(s.emails[:i], s.emails[i+1:]...)
			return true
		}
	}
	return false
}

func (s *Store) Clear() {
	s.mu.Lock()
	s.emails = nil
	s.mu.Unlock()
}

// ── WebSocket hub ─────────────────────────────────────────────────────────────

type Hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]bool
}

func NewHub() *Hub { return &Hub{clients: make(map[*websocket.Conn]bool)} }

func (h *Hub) Add(c *websocket.Conn)    { h.mu.Lock(); h.clients[c] = true; h.mu.Unlock() }
func (h *Hub) Remove(c *websocket.Conn) { h.mu.Lock(); delete(h.clients, c); h.mu.Unlock() }

type WSMessage struct {
	Type  string `json:"type"`
	Email *Email `json:"email,omitempty"`
}

func (h *Hub) Broadcast(msg WSMessage) {
	data, _ := json.Marshal(msg)
	h.mu.Lock()
	for c := range h.clients {
		_ = c.WriteMessage(websocket.TextMessage, data)
	}
	h.mu.Unlock()
}

// ── SMTP ──────────────────────────────────────────────────────────────────────

var idCounter atomic.Uint64

type Backend struct{ store *Store; hub *Hub }

func (b *Backend) NewSession(_ *gosmtp.Conn) (gosmtp.Session, error) {
	return &Session{store: b.store, hub: b.hub}, nil
}

type Session struct {
	store *Store
	hub   *Hub
	from  string
	to    []string
}

func (s *Session) AuthPlain(_, _ string) error                   { return nil }
func (s *Session) Mail(from string, _ *gosmtp.MailOptions) error { s.from = from; return nil }
func (s *Session) Rcpt(to string, _ *gosmtp.RcptOptions) error   { s.to = append(s.to, to); return nil }
func (s *Session) Reset()                                         { s.from = ""; s.to = nil }
func (s *Session) Logout() error                                  { return nil }

func (s *Session) Data(r io.Reader) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	email := parseEmail(s.from, s.to, raw)
	s.store.Add(email)
	s.hub.Broadcast(WSMessage{Type: "new_email", Email: email})
	log.Printf("recv from=%s to=%v subject=%q size=%d attachments=%d",
		email.From, email.To, email.Subject, email.Size, len(email.Attachments))
	return nil
}

// ── Email parsing ─────────────────────────────────────────────────────────────

type parseCtx struct {
	text        string
	html        string
	attachments []*Attachment
	cidMap      map[string]*Attachment
}

func parseEmail(envFrom string, envTo []string, raw []byte) *Email {
	e := &Email{
		ID:        fmt.Sprintf("%d", idCounter.Add(1)),
		From:      envFrom,
		To:        envTo,
		Timestamp: time.Now(),
		Size:      len(raw),
	}

	msg, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		e.Subject = "(parse error)"
		e.Text = string(raw)
		return e
	}

	dec := new(mime.WordDecoder)
	if subj, err := dec.DecodeHeader(msg.Header.Get("Subject")); err == nil && subj != "" {
		e.Subject = subj
	} else {
		e.Subject = "(no subject)"
	}
	if e.From == "" {
		e.From = msg.Header.Get("From")
	}

	ctx := &parseCtx{cidMap: make(map[string]*Attachment)}
	walkMIME(msg.Header.Get, msg.Body, ctx)

	e.Text = ctx.text
	e.HTML = replaceCIDs(ctx.html, ctx.cidMap)
	e.Attachments = ctx.attachments
	if e.Attachments == nil {
		e.Attachments = []*Attachment{}
	}
	return e
}

// walkMIME recursively walks a MIME tree, collecting text, HTML, and attachments.
func walkMIME(getHeader func(string) string, body io.Reader, ctx *parseCtx) {
	ct := getHeader("Content-Type")
	if ct == "" {
		ct = "text/plain"
	}
	mediaType, params, _ := mime.ParseMediaType(ct)

	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(body, params["boundary"])
		for {
			part, err := mr.NextPart()
			if err != nil {
				break
			}
			walkMIME(part.Header.Get, part, ctx)
		}
		return
	}

	cte := getHeader("Content-Transfer-Encoding")
	cd  := getHeader("Content-Disposition")
	cid := strings.Trim(getHeader("Content-Id"), "<> \t")

	r := decodeTransfer(body, cte)
	data, _ := io.ReadAll(r)

	disposition, cdParams, _ := mime.ParseMediaType(cd)
	isAttachment := strings.EqualFold(disposition, "attachment")

	// Body text parts
	if !isAttachment {
		if strings.HasPrefix(mediaType, "text/plain") && ctx.text == "" {
			ctx.text = string(data)
			return
		}
		if strings.HasPrefix(mediaType, "text/html") && ctx.html == "" {
			ctx.html = string(data)
			return
		}
	}

	// Skip empty non-attachment parts (e.g. bare multipart preambles)
	if len(data) == 0 && !isAttachment && cid == "" {
		return
	}

	// Everything else: attachment or inline resource
	filename := cdParams["filename"]
	if filename == "" {
		filename = params["name"]
	}
	if filename == "" && cid != "" {
		filename = strings.Split(cid, "@")[0]
	}
	if filename == "" {
		filename = "attachment" + typeExt(mediaType)
	}
	if decoded, err := new(mime.WordDecoder).DecodeHeader(filename); err == nil {
		filename = decoded
	}

	att := &Attachment{
		ID:          fmt.Sprintf("%d", len(ctx.attachments)),
		Filename:    filename,
		ContentType: mediaType,
		Size:        len(data),
		Inline:      cid != "" && !isAttachment,
		ContentID:   cid,
		Data:        data,
	}
	ctx.attachments = append(ctx.attachments, att)
	if cid != "" {
		ctx.cidMap[cid] = att
		if idx := strings.Index(cid, "@"); idx != -1 {
			ctx.cidMap[cid[:idx]] = att
		}
	}
}

func decodeTransfer(r io.Reader, enc string) io.Reader {
	switch strings.ToLower(strings.TrimSpace(enc)) {
	case "quoted-printable":
		return quotedprintable.NewReader(r)
	case "base64":
		// Email base64 has CRLF line breaks which standard decoder rejects — strip first.
		raw, _ := io.ReadAll(r)
		j := 0
		for _, b := range raw {
			if b != '\r' && b != '\n' && b != ' ' && b != '\t' {
				raw[j] = b
				j++
			}
		}
		raw = raw[:j]
		decoded := make([]byte, base64.StdEncoding.DecodedLen(len(raw)))
		n, _ := base64.StdEncoding.Decode(decoded, raw)
		return bytes.NewReader(decoded[:n])
	default:
		return r
	}
}

// replaceCIDs substitutes cid: references in HTML with inline data URIs.
func replaceCIDs(html string, cidMap map[string]*Attachment) string {
	for cid, att := range cidMap {
		dataURI := "data:" + att.ContentType + ";base64," + base64.StdEncoding.EncodeToString(att.Data)
		html = strings.ReplaceAll(html, "cid:"+cid, dataURI)
	}
	return html
}

func typeExt(mimeType string) string {
	switch mimeType {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "image/svg+xml":
		return ".svg"
	case "application/pdf":
		return ".pdf"
	case "text/plain":
		return ".txt"
	case "text/html":
		return ".html"
	case "text/csv":
		return ".csv"
	case "application/zip":
		return ".zip"
	case "application/gzip":
		return ".gz"
	case "application/msword":
		return ".doc"
	case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
		return ".docx"
	case "application/vnd.ms-excel":
		return ".xls"
	case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
		return ".xlsx"
	case "application/json":
		return ".json"
	}
	return ""
}

// ── Auth ──────────────────────────────────────────────────────────────────────

func basicAuth(user, pass string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != user || p != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="postroom"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── HTTP ──────────────────────────────────────────────────────────────────────

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func main() {
	store := NewStore()
	hub := NewHub()

	mux := http.NewServeMux()

	mux.HandleFunc("/logo.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(logoBytes)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	mux.HandleFunc("/api/emails", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(store.All())
		case http.MethodDelete:
			store.Clear()
			hub.Broadcast(WSMessage{Type: "cleared"})
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/emails/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/emails/")

		// Attachment sub-resource: {emailID}/attachments/{attID}
		if i := strings.Index(path, "/attachments/"); i != -1 {
			emailID := path[:i]
			attID := path[i+len("/attachments/"):]
			e := store.Get(emailID)
			if e == nil {
				http.NotFound(w, r)
				return
			}
			for _, att := range e.Attachments {
				if att.ID != attID {
					continue
				}
				ct := att.ContentType
				if ct == "" {
					ct = "application/octet-stream"
				}
				viewable := strings.HasPrefix(ct, "image/") ||
					ct == "application/pdf" ||
					strings.HasPrefix(ct, "text/")
				disp := "attachment"
				if viewable {
					disp = "inline"
				}
				w.Header().Set("Content-Type", ct)
				w.Header().Set("Content-Disposition",
					fmt.Sprintf(`%s; filename="%s"`, disp, att.Filename))
				w.Write(att.Data)
				return
			}
			http.NotFound(w, r)
			return
		}

		// Email operations
		id := path
		switch r.Method {
		case http.MethodGet:
			e := store.Get(id)
			if e == nil {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e)
		case http.MethodDelete:
			if !store.Delete(id) {
				http.NotFound(w, r)
				return
			}
			hub.Broadcast(WSMessage{Type: "deleted", Email: &Email{ID: id}})
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		hub.Add(conn)
		defer func() { hub.Remove(conn); conn.Close() }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	})

	smtpSrv := gosmtp.NewServer(&Backend{store: store, hub: hub})
	smtpSrv.Addr = ":1025"
	smtpSrv.Domain = "postroom.local"
	smtpSrv.MaxMessageBytes = 25 * 1024 * 1024
	smtpSrv.MaxRecipients = 100
	smtpSrv.AllowInsecureAuth = true

	go func() {
		log.Println("SMTP  :1025")
		if err := smtpSrv.ListenAndServe(); err != nil {
			log.Fatal("smtp:", err)
		}
	}()

	var handler http.Handler = mux
	if user, pass := os.Getenv("POSTROOM_USER"), os.Getenv("POSTROOM_PASS"); user != "" && pass != "" {
		handler = basicAuth(user, pass, mux)
		log.Printf("auth  enabled user=%s", user)
	}

	log.Println("HTTP  :8080  →  http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", handler))
}
