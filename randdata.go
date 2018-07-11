// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"encoding/json"
	"fmt"
	"github.com/icrowley/fake"
	"math/rand"
	"time"
)

func genRandomUsers(seed int64, array [][]byte) (int, error) {
	// We generate a per-item seed first, and then generate the users from
	// that so that we are able to increase the amount of data within each
	// user without breaking the data generated between versions.
	seedVals := rand.New(rand.NewSource(seed))

	totalBytes := 0
	for i := 0; i < len(array); i++ {
		itemSeed := seedVals.Int63()
		rand.Seed(itemSeed)
		fake.Seed(itemSeed)

		registerTime, _ := time.Parse("2006-01-02", fmt.Sprintf("%04d-%02d-%02d", fake.Year(1950, 2016), fake.MonthNum(), fake.Day()))
		user := map[string]interface{}{
			"id":       rand.Int(),
			"isActive": rand.Int()%2 == 0,
			"balance":  fake.Currency(),
			"picture":  fake.DomainName() + "." + fake.TopLevelDomain() + "/" + fake.CharactersN(8),
			"age":      20 + rand.Int31n(50),
			"eyeColor": fake.Color(),
			"name": map[string]interface{}{
				"first": fake.FirstName(),
				"last":  fake.LastName(),
			},
			"company":       fake.Company(),
			"email":         fake.EmailAddress(),
			"phone":         fake.Phone(),
			"address":       fake.StreetAddress(),
			"about":         fake.Paragraphs(),
			"registered":    registerTime,
			"tags":          nil,
			"friends":       nil,
			"greeting":      fake.Sentence(),
			"favoriteColor": fake.Color(),
		}

		tags := make([]string, 5)
		for j := 0; j < len(tags); j++ {
			tags[j] = fake.Word()
		}
		user["tags"] = tags

		friends := make([]map[string]interface{}, 10)
		for j := 0; j < len(friends); j++ {
			friends[j] = map[string]interface{}{
				"id":   rand.Int(),
				"age":  20 + rand.Int31n(50),
				"name": fake.FullName(),
			}
		}
		user["friends"] = friends

		data, err := json.Marshal(user)
		if err != nil {
			return totalBytes, err
		}
		totalBytes += len(data)
		array[i] = data
	}

	return totalBytes, nil
}
