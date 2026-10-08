package openmeteo

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type client struct {
	httpClient *http.Client
}
type Response struct {
	Current struct {
		Time          string  `json:"time"`
		Temperature2m float64 `json:"temperature_2m"`
	}
}

func NewClient(httpClient *http.Client) *client {
	return &client{httpClient: httpClient}
}

func (c *client) GetTemperature(lat, lon float64) (Response, error) {
	resp, err := c.httpClient.Get(fmt.Sprintf("https://api.open-meteo.com/v1/forecast?latitude=%f&longitude=%f&current=temperature_2m", lat, lon))
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Response{}, fmt.Errorf("status code: %s", resp.Status)
	}

	var r Response
	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return Response{}, err
	}
	return r, nil
}
