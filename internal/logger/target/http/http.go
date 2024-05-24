package http

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	yhttp "github.com/minio/minio/internal/http"
	xnet "github.com/minio/pkg/v2/net"
)

// Config http logger target
type Config struct {
	Enabled    bool              `json:"enabled"`
	Name       string            `json:"name"`
	UserAgent  string            `json:"userAgent"`
	Endpoint   *xnet.URL         `json:"endpoint"`
	AuthToken  string            `json:"authToken"`
	ClientCert string            `json:"clientCert"`
	ClientKey  string            `json:"clientKey"`
	BatchSize  int               `json:"batchSize"`
	QueueSize  int               `json:"queueSize"`
	QueueDir   string            `json:"queueDir"`
	Proxy      string            `json:"string"`
	Transport  http.RoundTripper `json:"-"`

	// Custom logger
	LogOnceIf func(ctx context.Context, err error, id string, errKind ...interface{}) `json:"-"`
}

type HTTPTrgt struct {
	config         *Config
	payloadType    string
	client         *http.Client
	ctx            context.Context
	requestTimeout time.Duration
}

func NewHTTPTarget(ctx context.Context, config *Config) (T *HTTPTrgt, err error) {
	if config.Proxy != "" {
		proxyURL, err := url.Parse(config.Proxy)
		if err != nil {
			return nil, err
		}
		transport := config.Transport
		ctransport := transport.(*http.Transport).Clone()
		ctransport.Proxy = http.ProxyURL(proxyURL)
		config.Transport = ctransport
	}

	// TODO .. make configuration variable
	transport := config.Transport
	ctransport := transport.(*http.Transport).Clone()
	ctransport.TLSClientConfig.InsecureSkipVerify = true
	config.Transport = ctransport
	// ............

	T = &HTTPTrgt{
		client: &http.Client{
			Transport: config.Transport,
		},
		ctx:            ctx,
		config:         config,
		requestTimeout: 3 * time.Second,
		payloadType:    "application/json",
	}

	if config.BatchSize > 1 {
		T.payloadType = ""
	}
	return
}

func (h *HTTPTrgt) Write(payload []byte) (n int, err error) {
	ctx, cancel := context.WithTimeout(h.ctx, h.requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		h.config.Endpoint.String(),
		bytes.NewReader(payload),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"invalid configuration for '%s'; %v",
			h.config.Endpoint.String(),
			err,
		)
	}

	if h.payloadType != "" {
		req.Header.Set(yhttp.ContentType, h.payloadType)
	}

	if h.config.AuthToken != "" {
		req.Header.Set("Authorization", h.config.AuthToken)
	}

	req.Header.Set(yhttp.MinIOVersion, yhttp.GlobalMinIOVersion)
	req.Header.Set(yhttp.MinioDeploymentID, yhttp.GlobalDeploymentID)
	req.Header.Set("User-Agent", h.config.UserAgent)

	resp, err := h.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf(
			"%s returned '%w', please check your endpoint configuration",
			h.config.Endpoint.String(),
			err,
		)
	}

	yhttp.DrainBody(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		return 0, nil
	case http.StatusForbidden:
		return 0, fmt.Errorf(
			"%s returned '%s', please check if your auth token is correctly set",
			h.config.Endpoint.String(),
			resp.Status,
		)
	default:
		return 0, fmt.Errorf(
			"%s returned '%s', please check your endpoint configuration",
			h.config.Endpoint.String(),
			resp.Status,
		)
	}
}
