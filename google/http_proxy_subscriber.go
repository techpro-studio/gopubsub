package google

import (
	"context"
	"errors"
	"github.com/go-jose/go-jose/v4/json"
	"github.com/techpro-studio/gohttplib"
	"github.com/techpro-studio/gopubsub/abstract"
	"io"
	"log"
	"net/http"
)

type HttpProxySubscriber struct {
	port   string
	server *http.Server
}

func NewHttpProxySubscriber(port string) *HttpProxySubscriber {
	return &HttpProxySubscriber{port: port}
}

type WrappedMessage struct {
	Message struct {
		Data []byte `json:"data,omitempty"`
		ID   string `json:"id"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

func (h *HttpProxySubscriber) Close() error {
	return h.server.Shutdown(context.Background())
}

func (h *HttpProxySubscriber) Listen(ctx context.Context, handler abstract.SubscriptionHandler) {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var m WrappedMessage
		body, err := io.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			log.Printf("Error reading body: %v", err)
			gohttplib.HTTP400("Failed to read request body").Write(w)
			return
		}
		// byte slice unmarshalling handles base64 decoding.
		if err := json.Unmarshal(body, &m); err != nil {
			log.Printf("Error unmarshalling body: %v", err)
			gohttplib.HTTP400("Failed to unmarshal message").Write(w)
			return
		}

		var payload any
		err = json.Unmarshal(m.Message.Data, &payload)
		if err != nil {
			log.Printf("Failed to unmarshal payload: %v", err)
			gohttplib.HTTP400("Failed to unmarshal payload").Write(w)
			return
		}

		err = handler.Handle(ctx, payload)
		gohttplib.WriteJsonOrError(w, map[string]int{"ok": 1}, 200, err)
	})
	h.server = &http.Server{
		Addr:    ":" + h.port,
		Handler: mux,
	}
	log.Printf("Listening on port %s", h.port)

	// Start server (blocking)
	if err := h.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
