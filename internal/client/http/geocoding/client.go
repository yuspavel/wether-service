package geocoding

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type client struct {
	httpClient *http.Client
}
type Response struct {
	Name      string  `json:"name"`
	Country   string  `json:"country"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func NewClient(c *http.Client) *client {
	return &client{httpClient: c}
}

func (c *client) GetCoords(city string) (Response, error) {
	resp, err := c.httpClient.Get(fmt.Sprintf("https://geocoding-api.open-meteo.com/v1/search?name=%s&count1&language=ru&format=json", city))
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Response{}, errors.New(resp.Status)
	}

	var geoResp struct { //Временная структура соответствующая ответу api и содержащая все поля вложенной структуры. Пример структуры можно посмотреть вызвав api https://geocoding-api.open-meteo.com/v1/search?name=Moscow&count1&language=ru&format=json
		Result []Response `json:"results"`
	}

	err = json.NewDecoder(resp.Body).Decode(&geoResp)
	if err != nil {
		return Response{}, err
	}
	return geoResp.Result[0], nil
}
