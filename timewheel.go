package timewheel

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
)

type (
	Execute func(k interface{})
	Run     func()
)

const poolCapacity = 15

type Task struct {
	key     interface{}   //用来唯一标识任务
	value   interface{}   //执行时所需参数
	execute Execute       //每个任务自己的回调函数
	circle  int           //周期数
	times   int           //该任务需要执行的次数
	delay   time.Duration //延迟时间
}

type TimeWheel struct {
	interval       time.Duration       //每个槽位代表的时间间隔
	ticker         *time.Ticker        //触发器
	slots          []*list.List        //存放定时任务的时间轮数组
	timers         map[interface{}]int //定时任务所在的槽位
	currentPos     int                 //当前指针所在的位置
	numSlots       int                 //时间轮槽位数量
	execute        Execute             //公共执行函数
	setChannel     chan *Task          //增加任务的chan
	delChannel     chan interface{}    //删除任务的chan
	modifyTWStatus chan struct{}       //停止整个时间轮
	pool           *ants.PoolWithFunc  //协程池
	wg             sync.WaitGroup
}

// 创建TimeWheel
func NewTimeWheel(slots int, execute Execute) (*TimeWheel, error) {
	if slots <= 0 {
		return nil, fmt.Errorf("slots must be greater than 0")
	}

	t := &TimeWheel{
		interval:       time.Second,
		ticker:         time.NewTicker(time.Second),
		slots:          make([]*list.List, slots),
		timers:         make(map[interface{}]int),
		currentPos:     0,
		numSlots:       slots,
		execute:        execute,
		setChannel:     make(chan *Task),
		delChannel:     make(chan interface{}),
		modifyTWStatus: make(chan struct{}),
		wg:             sync.WaitGroup{},
	}
	p, err := ants.NewPoolWithFunc(poolCapacity, func(i interface{}) {
		executeTask(i)
		t.wg.Done()
	})
	if err != nil {
		return nil, err
	}

	t.pool = p
	t.initSlots()
	go t.startTimeWheel()
	return t, nil
}

func NewTask(key, value interface{}, execute Execute, times int, delay time.Duration) *Task {
	if key == nil {
		return nil
	}

	return &Task{
		key:     key,
		value:   value,
		execute: execute,
		times:   times,
		delay:   delay,
	}
}

func (t *TimeWheel) initSlots() {
	for i := 0; i < t.numSlots; i++ {
		t.slots[i] = list.New()
	}
}

// 启动TimeWheel
func (t *TimeWheel) startTimeWheel() {
	for {
		select {
		case <-t.ticker.C:
			t.checkAndRunTask()
		case task := <-t.setChannel:
			t.setTask(task)
		case task := <-t.delChannel:
			t.deleteTask(task)
		case <-t.modifyTWStatus:
			t.stopTimeWheel()
		}
	}
}

// 时间轮的移动
func (t *TimeWheel) checkAndRunTask() {
	var tasks []Run
	var r Run
	currentList := t.slots[t.currentPos]
	//扫描任务链表，对到点的执行
	for l := currentList.Front(); l != nil; {
		task := l.Value.(*Task)
		//如果当前的轮转周期大于0，周期数减一
		if task.circle > 0 {
			task.circle--
			l = l.Next()
			continue
		}

		r = buildRun(t.execute, task)
		tasks = append(tasks, r)

		//任务执行完之后将其从时间轮中删除
		next := l.Next()
		delete(t.timers, task.key)
		currentList.Remove(l)
		l = next

		task.times--
		if task.times > 0 {
			t.setTask(task)
		}
	}

	t.currentPos = (t.currentPos + 1) % t.numSlots
	//并发执行
	t.runTasks(tasks)
}

func (t *TimeWheel) SetTask(task *Task) {
	if task == nil || task.delay < 0 {
		return
	}
	if _, ok := t.timers[task.key]; ok {
		logrus.Error("Repeat the key, please make sure the key is unique")
		return
	}
	t.setChannel <- task
}

func (t *TimeWheel) setTask(task *Task) {
	pos, circle := t.getPositionAndCircle(task.delay)
	task.circle = circle
	t.slots[pos].PushBack(task)
	if task.key != nil {
		t.timers[task.key] = pos
	}
}

func (t *TimeWheel) getPositionAndCircle(delay time.Duration) (int, int) {
	delaySeconds := delay.Seconds()
	intervalSeconds := t.interval.Seconds()
	circle := int(delaySeconds/intervalSeconds) / t.numSlots
	position := (t.currentPos + int(delaySeconds/intervalSeconds)) % t.numSlots
	return position, circle
}

func (t *TimeWheel) DeleteTask(key interface{}) {
	if key == nil {
		return
	}
	t.delChannel <- key
}

func (t *TimeWheel) deleteTask(key interface{}) {
	pos, ok := t.timers[key]
	if !ok {
		return
	}

	tasks := t.slots[pos]
	for l := tasks.Front(); l != nil; {
		e := l.Value.(*Task)
		if e.key == key {
			tasks.Remove(l)
			delete(t.timers, key)
		}
		l = l.Next()
	}
}

func (t *TimeWheel) StopTimeWheel() {
	t.modifyTWStatus <- struct{}{}
}

func (t *TimeWheel) stopTimeWheel() {
	t.ticker.Stop()
}

func (t *TimeWheel) runTasks(tasks []Run) {
	if len(tasks) == 0 {
		return
	}

	for _, task := range tasks {
		t.wg.Add(1)
		t.pool.Invoke(task)
	}
}

func buildRun(tw Execute, task *Task) Run {
	if tw == nil && task.execute == nil {
		logrus.Error("global's execute and task's execute is nil")
		return nil
	}

	return func() {
		if task.execute != nil {
			task.execute(task.value)
		}
		if tw != nil {
			tw(task.value)
		}
	}
}

func executeTask(i interface{}) {
	defer func() {
		if err := recover(); err != nil {
			logrus.Error(err)
		}
	}()

	run := i.(Run)
	run()
}
