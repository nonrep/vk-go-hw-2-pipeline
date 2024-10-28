package main

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nonrep/go-homework-2-pipeline/semaphore"
	uniqueSet "github.com/nonrep/go-homework-2-pipeline/uniqueSet"
)

// cat emails.txt | SelectUsers | SelectMessages | CheckSpam | CombineResults

func RunPipeline(cmds ...cmd) {
	in := make(chan interface{})

	for _, cmd := range cmds {
		out := make(chan interface{})

		go cmd(in, out)

		in = out
	}

	go func() {
		time.Sleep(10 * time.Second)
		close(in)
	}()
}

// in - string
// out - User
func SelectUsers(in, out chan interface{}) {
	var wg sync.WaitGroup
	mu := &sync.Mutex{}
	uniqSet := uniqueSet.New()

	for email := range in {
		wg.Add(1)
		go func(email interface{}, mu *sync.Mutex) {
			defer wg.Done()

			user := GetUser(email.(string))

			if !uniqSet.Exists(user.Email) {
				mu.Lock()
				uniqSet.Add(user.Email)
				mu.Unlock()

				out <- user
			}
		}(email, mu)
	}

	wg.Wait()
	close(out)
}

// in - User
// out - MsgID
func SelectMessages(in, out chan interface{}) {
	var users []User
	wg := &sync.WaitGroup{}

	for user := range in {
		users = append(users, user.(User))

		if len(users) == 2 {
			wg.Add(1)
			go processUsers(append([]User{}, users...), out, wg)
			users = users[:0]
		}

	}

	if len(users) > 0 {
		wg.Add(1)
		go processUsers(append([]User{}, users...), out, wg)
	}

	wg.Wait()
	close(out)
}

func processUsers(users []User, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	MsgIDs, err := GetMessages(users...)
	if err != nil {
		fmt.Println("SelectMessages не удалось получить сообщения: ", err)
		return
	}

	for _, MsgID := range MsgIDs {
		out <- MsgID
	}
}

// in - MsgID
// out - MsgData
func CheckSpam(in, out chan interface{}) {
	const connCount = 5
	sem := semaphore.New(connCount)
	wg := &sync.WaitGroup{}

	for msgID := range in {
		wg.Add(1)
		go func(msgID MsgID) {
			defer wg.Done()
			sem.Acquire()
			defer sem.Release()

			hasSpam, err := HasSpam(msgID)
			if err != nil {
				fmt.Println("CheckSpam не удалось проверить сообщение на спам: ", err)
				return
			}

			msg := MsgData{
				ID:      msgID,
				HasSpam: hasSpam,
			}

			out <- msg

		}(msgID.(MsgID))
	}

	wg.Wait()
	close(out)
}

// in - MsgData
// out - string
func CombineResults(in, out chan interface{}) {
	var result []MsgData
	mu := &sync.Mutex{}

	for msgData := range in {
		go func() {
			mu.Lock()
			defer mu.Unlock()

			result = append(result, msgData.(MsgData))
		}()
	}

	sort.Sort(BySpamAndID(result))

	for _, msg := range result {
		out <- fmt.Sprintln(msg.HasSpam, msg.ID)
	}

	close(out)
}
