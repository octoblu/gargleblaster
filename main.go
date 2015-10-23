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

// Job stores a job
type Job struct {
  ResponseUUID string `json:"responseUuid"`
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

func getJob(redisConn redis.Conn, timeout int) (*Job, error) {
  var job Job

  result,err := redisConn.Do("BRPOP", "meshblu:authenticate:queue", 100)
  if err != nil {
    return nil, err
  }

  if result == nil {
    return nil, nil
  }

  results := result.([]interface{})
  jobBytes := results[1].([]byte)

  err = json.Unmarshal(jobBytes, &job)
  return &job, err
}

func doJob(redisConn redis.Conn, job *Job, authenticated string) error {
  if authenticated == "random" {
    list := []string{"true", "false"}
    randIndex := rand.Intn(len(list))
    authenticated = list[randIndex]
  }
  key := fmt.Sprintf("meshblu:authenticate:%v", job.ResponseUUID)
  value := fmt.Sprintf(`[null, {"authenticated": %v}]`, authenticated)
  _,err := redisConn.Do("LPUSH", key, value)
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
    job, err := getJob(redisConn, context.Int("timeout"))
    if err != nil {
      log.Fatalf("getJob error: %v", err.Error())
    }
    if job == nil {
      continue
    }

    log.Printf("Got a job: %v", job.ResponseUUID)

    err = doJob(redisConn, job, context.String("authenticated"))
    if err != nil {
      log.Fatalf("doJob error: %v", err.Error())
    }
  }
}
