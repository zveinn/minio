package http

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	yhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger/target/types"
	xnet "github.com/minio/pkg/v3/net"
)

// Config http logger target
type Config struct {
	Enabled          bool              `json:"enabled"`
	Name             string            `json:"name"`
	UserAgent        string            `json:"userAgent"`
	Endpoint         *xnet.URL         `json:"endpoint"`
	AuthToken        string            `json:"authToken"`
	TLSSkipVerify    bool              `json:"tlsSkipVerify"`
	Timeout          int               `json:"timeout"`
	Encoding         types.EncoderType `json:"encoding"`
	ClientCert       string            `json:"clientCert"`
	ClientKey        string            `json:"clientKey"`
	BatchSize        int               `json:"batchSize"`
	BatchPayloadSize int               `json:"batchPayloadSize"`
	QueueSize        int               `json:"queueSize"`
	QueueDir         string            `json:"queueDir"`
	Proxy            string            `json:"string"`
	Transport        http.RoundTripper `json:"-"`
	MaxRetry         int               `json:"maxRetry"`
	RetryIntvl       time.Duration     `json:"retryInterval"`

	// Custom logger
	// LogOnceIf func(ctx context.Context, err error, id string, errKind ...interface{}) `json:"-"`
}

type HTTPTrgt struct {
	config         *Config
	payloadType    string
	client         *http.Client
	ctx            context.Context
	requestTimeout time.Duration
}

func NewHTTPTarget(ctx context.Context, config *Config) (T *HTTPTrgt, err error) {
	ctransport := config.Transport.(*http.Transport).Clone()

	if config.Proxy != "" {
		proxyURL, err := url.Parse(config.Proxy)
		if err != nil {
			return nil, err
		}
		ctransport.Proxy = http.ProxyURL(proxyURL)
	}

	if config.TLSSkipVerify {
		ctransport.TLSClientConfig.InsecureSkipVerify = true
	}

	var contentType string
	switch config.Encoding {
	case types.EncoderCBOR:
		contentType = "application/cbor"
	case types.EncoderJSON:
		contentType = "application/json"
		if config.BatchSize > 1 {
			contentType = ""
		}
	default:
		contentType = "text/plain"
	}

	config.Transport = ctransport

	T = &HTTPTrgt{
		client: &http.Client{
			Transport: config.Transport,
		},
		ctx:            ctx,
		config:         config,
		requestTimeout: time.Duration(config.Timeout) * time.Millisecond,
		payloadType:    contentType,
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
	default:
		return 0, fmt.Errorf(
			"%s returned '%s', please check your endpoint configuration",
			h.config.Endpoint.String(),
			resp.Status,
		)
	}
}
