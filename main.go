package main

import (
  "encoding/json"
  "fmt"
  "log"
  "math/rand"
  "os"
  "github.com/codegangsta/cli"
  "github.com/garyburd/redigo/redis"
)

// Request stores Request
type Request struct {
  Key string
  Metadata Metadata
}

// Metadata stores Metadata
type Metadata struct {
  ResponseID string `json:"responseId"`
}

func main() {
  app := cli.NewApp()
  app.Name = "redis-interval-work-queue"
  app.Usage = "Run the work queue"
  app.Action = doNothing
  app.Flags = []cli.Flag {
    cli.StringFlag{
      Name: "authenticated, a",
      Value: "true",
      Usage: "What to return can be: true/false/random",
    },
    cli.IntFlag{
      Name: "timeout, t",
      Value: 100,
      Usage: "BRPOP timeout",
    },
  }
  app.Run(os.Args)
}

func getRequest(redisConn redis.Conn, timeout int) (*Request, error) {
  result,err := redisConn.Do("BRPOP", "meshblu:authenticate:queue", 100)
  if err != nil {
    return nil, err
  }

  if result == nil {
    return nil, nil
  }

  results := result.([]interface{})
  requestKey := results[1].(string)

  metadataBytes,err := redisConn.Do("HGET", requestKey, "request:metadata")

  var metadata Metadata

  err = json.Unmarshal(metadataBytes.([]byte), &metadata)
  if err != nil {
    return nil, err
  }

  return &Request{Key:requestKey, Metadata:metadata}, nil
}

func responseToRequest(redisConn redis.Conn, request *Request, authenticated string) error {
  var err error
  if authenticated == "random" {
    list := []string{"true", "false"}
    randIndex := rand.Intn(len(list))
    authenticated = list[randIndex]
  }

  metadataStr := fmt.Sprintf(`{"responseId": "%v", "code": 200, "status": "OK"}`, request.Metadata.ResponseID)
  dataStr := fmt.Sprintf(`{"authenticated": %v}`, authenticated)

  responseKey := fmt.Sprintf(`meshblu:internal:authenticate:%v`, request.Metadata.ResponseID)
  _,err = redisConn.Do("HSET", request.Key, "response:metadata", metadataStr)
  if err == nil {
    return err
  }
  _,err = redisConn.Do("HSET", request.Key, "response:data", dataStr)
  if err == nil {
    return err
  }
  _,err = redisConn.Do("LPUSH", responseKey, request.Key)
  return err
}

func doNothing(context *cli.Context) {
  redisConn, err := redis.Dial("tcp", ":6379")
  if err != nil {
    log.Fatalf("Redis Connect error: %v", err.Error())
  }
  defer redisConn.Close()

  for {
    log.Print("Waiting for a job")
    request, err := getRequest(redisConn, context.Int("timeout"))
    if err != nil {
      log.Fatalf("getRequest error: %v", err.Error())
    }
    if request == nil {
      continue
    }

    log.Printf("Got a job: %v", request.Key)

    err = responseToRequest(redisConn, request, context.String("authenticated"))
    if err != nil {
      log.Fatalf("responseToRequest error: %v", err.Error())
    }
  }
}
