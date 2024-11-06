package main

import (
	"fmt"
	"sort"
	"sync"

	uniqueSet "github.com/nonrep/go-homework-2-pipeline/unique_set"
	errGroup "golang.org/x/sync/errgroup"
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
	mu := &sync.RWMutex{}
	uniqSet := uniqueSet.New()

	for email := range in {
		wg.Add(1)
		go func(email interface{}) {
			defer wg.Done()

			parsedEmail, ok := email.(string)
			if !ok {
				fmt.Println("email не является строкой")
				return
			}

			user := GetUser(parsedEmail)

			mu.RLock()
			if uniqSet.Exists(user.Email) {
				mu.RUnlock()
				return
			}
			mu.RUnlock()

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
	var g errGroup.Group
	g.SetLimit(connCount)

	for msgID := range in {
		msgID := msgID.(MsgID)
		g.Go(func() error {
			hasSpam, err := HasSpam(msgID)
			if err != nil {
				return err
			}

			msg := MsgData{
				ID:      msgID,
				HasSpam: hasSpam,
			}

			out <- msg
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		fmt.Printf("CheckSpam не удалось проверить сообщение на спам: %v", err)
	}
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
		parsedMsgData, ok := msgData.(MsgData)
		if !ok {
			fmt.Println("msgData не является экземпляров структуры MsgData")
			return
		}

		go func(data MsgData) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()

			result = append(result, data)
		}(parsedMsgData)
	}

	wg.Wait()

	sort.Sort(MsgDataSorter(result))

	for _, msg := range result {
		out <- fmt.Sprintf("%t %d", msg.HasSpam, msg.ID)
	}

}
