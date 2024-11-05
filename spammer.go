package main

import (
	"fmt"
	"sort"
	"sync"

	"github.com/nonrep/go-homework-2-pipeline/semaphore"
	uniqueSet "github.com/nonrep/go-homework-2-pipeline/uniqueSet"
)

// RunPipeline получает команды, выход одной команды передаестя на вход другой.
func RunPipeline(cmds ...cmd) {
	in := make(chan interface{})
	wg := &sync.WaitGroup{}

	for _, cmd := range cmds {
		out := make(chan interface{})

		wg.Add(1)

		go func(in, out chan interface{}) {
			defer wg.Done()
			defer close(out)
			cmd(in, out)
		}(in, out)

		in = out
	}
	wg.Wait()
}

// SelectUsers получает емайлы юзеров и передает информацию о юзере.
//
// in - string (email юзера);
// out - User (структура юзера)
func SelectUsers(in, out chan interface{}) {
	var wg sync.WaitGroup
	mu := &sync.Mutex{}
	uniqSet := uniqueSet.New()

	for email := range in {
		wg.Add(1)
		go func(email interface{}) {
			defer wg.Done()

			user := GetUser(email.(string))

			mu.Lock()
			defer mu.Unlock()

			if !uniqSet.Exists(user.Email) {
				uniqSet.Add(user.Email)
				out <- user
			}
		}(email)
	}

	wg.Wait()
}

// SelectMessages получает юзеров, передает парами в processUsers, возвращает ID сообщений юзеров.
//
// in - User (структура юзера)
// out - MsgID (ID сообщения)
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
}

// processUsers получает юзеров и передает ID их сообщений.
//
// out - MsgID (ID сообщения)
func processUsers(users []User, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	msgIDs, err := GetMessages(users...)
	if err != nil {
		fmt.Printf("SelectMessages не удалось получить сообщения: %v", err)
		return
	}

	for _, msgID := range msgIDs {
		out <- msgID
	}
}

// CheckSpam получает ID сообщения и передает информацию о сообщении.
//
// in - MsgID (ID сообщения)
// out - MsgData (структура сообщения)
func CheckSpam(in, out chan interface{}) {
	// HasSpam может обрабатывать одновременно только 5 соединений.
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
				fmt.Printf("CheckSpam не удалось проверить сообщение на спам: %v", err)
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
}

// CombineResults получает данные сообщений и передает информацию о них в виде строки.
//
// in - MsgData (структура сообщения)
// out - string (информация о сообщении: HasSpam и ID)
func CombineResults(in, out chan interface{}) {
	var result []MsgData
	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	for msgData := range in {
		wg.Add(1)
		go func(data MsgData) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()

			result = append(result, data)
		}(msgData.(MsgData))
	}

	wg.Wait()

	sort.Sort(BySpamAndID(result))

	for _, msg := range result {
		out <- fmt.Sprintf("%t %d", msg.HasSpam, msg.ID)
	}

}
