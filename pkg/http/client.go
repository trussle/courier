package http

import (
	"bytes"
	"net/http"

	"github.com/pkg/errors"
)

// Client represents a http client that has a one to one relationship with a url
type Client struct {
	client *http.Client
	url    string
}

// NewClient creates a Client with the http.Client and url
func NewClient(client *http.Client, url string) *Client {
	return &Client{client, url}
}

// Send a request to the url associated.
// If the response returns anything other than a StatusOK (200), then it
// will return an error.
func (c *Client) Send(p []byte) error {
	resp, err := c.client.Post(c.url, "application/binary", bytes.NewReader(p))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("invalid status code: %d", resp.StatusCode)
	}

	return nil
}
