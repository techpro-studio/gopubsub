package google

import (
	"context"
	"github.com/go-jose/go-jose/v4/json"
	"github.com/techpro-studio/gohttplib"
	"github.com/techpro-studio/gopubsub/abstract"
	"io"
	"log"
	"net/http"
)

type HttpProxySubscriber struct {
	port string
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

func (h *HttpProxySubscriber) Listen(ctx context.Context, handler abstract.SubscriptionHandler) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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
	// Determine port for HTTP service.

	// Start HTTP server.
	log.Printf("Listening on port %s", h.port)
	if err := http.ListenAndServe(":"+h.port, nil); err != nil {
		log.Fatal(err)
	}
}
