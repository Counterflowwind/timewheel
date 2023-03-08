package timewheel

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestNewTimeWheel(t *testing.T) {
	timeWheel, err := NewTimeWheel(15, func(k interface{}) {
		fmt.Println("this is global execute ", k)
	})
	if err != nil {
		log.Fatal(err)
	}

	value := 3
	task := NewTask("pjytest", &value, func(k interface{}) {
		num := k.(*int)
		*num++
		fmt.Println(*num)
	}, 20, time.Second*5)
	timeWheel.SetTask(task)

	task2 := NewTask("pjytest2", "pjy666", func(k interface{}) {
		str := k.(string)
		fmt.Println(str)
	}, 4, time.Second*2)
	timeWheel.SetTask(task2)

	value2 := 80
	task3 := NewTask("pjytest3", &value2, func(k interface{}) {
		num := k.(*int)
		*num++
		fmt.Println(*num)
	}, 20, time.Second*5)
	timeWheel.SetTask(task3)

	time.Sleep(time.Second * 7)
	timeWheel.DeleteTask("pjytest2")
	//timeWheel.StopTimeWheel()
	time.Sleep(time.Minute * 2)
}
