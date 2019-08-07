/* Copyright(C) 2019 MariaDB Corporation.

This program is free software; you can redistribute itand /or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02111 - 1301 USA*/

#include "tpool_structs.h"
#include <limits.h>
#include <algorithm>
#include <assert.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <limits.h>
#include <mutex>
#include <queue>
#include <stack>
#include <thread>
#include <vector>
#include "tpool.h"
#include <assert.h>


namespace tpool
{

#ifdef __linux__
  extern aio* create_linux_aio(thread_pool* tp, int max_io);
#endif
#ifdef _WIN32
  extern aio* create_win_aio(thread_pool* tp, int max_io);
#endif

  static const std::chrono::milliseconds LONG_TASK_DURATION = std::chrono::milliseconds(500);

/**
  Implementation of generic threadpool.
  This threadpool consists of the following components

  - The task queue. This queue is populated by submit()
  - Worker that execute the  work items.
  - Timer thread that takes care of pool health
 
  The task queue is populated by submit() method.
  on submit(), a worker thread  can be woken, or created
  to execute tasks.

  The timer thread watches if work items  are being dequeued, and if not, 
  this can indicate potential deadlock.
  Thus the timer thread can also wake or create a thread, to ensure some progress.

  Optimizations:

  - worker threads that are idle for long time will shutdown.
  - worker threads are woken in LIFO order, which minimizes context switching
  and also ensures that idle timeout works well. LIFO wakeup order ensures
  that active threads stay active, and idle ones stay idle.

  - to minimize spurious wakeups, some items are not put into the queue. Instead
  submit() will pass the data directly to the thread it woke up.
*/
class thread_pool_generic : public thread_pool
{
  /**
   Worker wakeup flags.
  */
  enum worker_wake_reason
  {
    WAKE_REASON_NONE,
    WAKE_REASON_TASK,
    WAKE_REASON_SHUTDOWN
  };
  /* A per-worker  structure.*/
  struct worker_data
  {
    /** Condition variable to wakeup this worker.*/
    std::condition_variable m_cv;

    /** Reason why worker was woken. */
    worker_wake_reason m_wake_reason;

    /** 
      If worker wakes up with WAKE_REASON_TASK, this the task it needs to execute.
    */
    task m_task;
    worker_data *m_prev;
    worker_data *m_next;
    bool m_is_executing_task;
    bool m_is_long_task;
    std::chrono::system_clock::time_point m_task_start_time;
    worker_data() :
      m_cv(),
      m_wake_reason(WAKE_REASON_NONE),
      m_task{ 0, 0 },
      m_prev(),
      m_next(),
      m_is_executing_task(),
      m_is_long_task(),
      m_task_start_time()
      {}
  };
  cache<worker_data> m_thread_data_cache;
  /** The task queue */
  circular_queue<task> m_task_queue;

  /* List of standby (idle) workers.*/
  doubly_linked_list<worker_data> m_standby_threads;

  /** List of threads that are executing tasks. */
  doubly_linked_list<worker_data> m_active_threads;

  /* Mutex that protects the whole struct, most importantly 
  the standby threads list, and task queue. */
  std::mutex m_mtx;

  /** Timeout after which idle worker shuts down.*/
  std::chrono::milliseconds m_thread_timeout;

  /** How often should timer wakeup.*/
  std::chrono::milliseconds m_timer_interval;

  /** Condition variable, used in pool shutdown-*/
  std::condition_variable m_cv_no_active_threads;
  /** Another condition variable, used in pool shutdown-*/
  std::condition_variable m_cv_no_threads;

  /** Condition variable to signal that task queue is not full*/
  std::condition_variable m_cv_queue_not_full;

  /** Condition variable for the timer thread. Signaled on shutdown.*/
  std::condition_variable m_cv_timer;

  /** The timer thread. Will be join()ed on shutdown.*/
  std::thread m_timer_thread;

  /** Overall number of dequeued tasks. */
  int m_tasks_dequeued;

  /**Statistic related, number of worker thread wakeups.*/
  int m_wakeups;

  /** 
  Statistic related, number of spurious thread wakeups
  (i.e thread woke up, and the task queue is empty)
  */
  int m_spurious_wakeups;

  /**
  Number of long executing tasks
  */
  /**  The desired concurrency.  This number of workers should be actively executing.*/
  unsigned int m_concurrency;

  /** True, if threadpool is being shutdown, false otherwise */
  bool m_in_shutdown;

  /**
    Whether this threadpool uses a timer. With permanent pool (fixed number of
    workers, timer is not started, and m_timer_on will be false.
  */
  bool m_timer_on;
  /** Last time timer ran, used as very coarse clock. */
  std::chrono::system_clock::time_point m_timestamp;
  /** Number of long running tasks. The long running tasks are excluded when 
  adjusting concurrency */
  int m_long_tasks_count;
  std::chrono::system_clock::time_point m_last_thread_creation;
  /** Minimumal number of threads in this pool.*/
  unsigned int m_min_threads;

  /** Maximal number of threads in this pool. */
  unsigned int m_max_threads;

  void worker_main(worker_data *thread_data);
  void worker_end(worker_data* thread_data);
  void timer_main();
  bool add_thread();
  bool wake(worker_wake_reason reason, const task *t= nullptr);
  void wake_or_create_thread();
  bool get_task(worker_data *thread_var, task *t);
  bool wait_for_tasks(std::unique_lock<std::mutex> &lk,
                      worker_data *thread_var);
  void timer_start();
  void timer_stop();
  size_t thread_count()
  {
    return m_active_threads.size() + m_standby_threads.size();
  }
public:
  thread_pool_generic(int min_threads, int max_threads);
  ~thread_pool_generic();
  void submit_task(const task &task) override;
  virtual aio *create_native_aio(int max_io) override
  {
#ifdef _WIN32
    return create_win_aio(this, max_io);
#elif defined(__linux__)
    return create_linux_aio(this,max_io);
#else
    return nullptr;
#endif
  }
};

/**
  Register worker in standby list, and wait to be woken.

  @return 
  true  -  thread was woken
  false -  idle wait timeout exceeded (the current thread need to shutdown)
*/
bool thread_pool_generic::wait_for_tasks(std::unique_lock<std::mutex> &lk,
                                   worker_data *thread_data)
{
  assert(m_task_queue.empty());
  assert(!m_in_shutdown);

  thread_data->m_wake_reason= WAKE_REASON_NONE;
  m_active_threads.erase(thread_data);
  m_standby_threads.push_back(thread_data);

  for (;;)
  {
    thread_data->m_cv.wait_for(lk, m_thread_timeout);
    if (thread_data->m_wake_reason != WAKE_REASON_NONE)
    {
      return true;
    }

    if (thread_count() <= m_min_threads)
    {
      continue;
    }

    /*
      Woke up due to timeout, remove this thread's  from the standby list. In
      all other cases where it is signaled it is removed by the signaling
      thread.
    */
    m_standby_threads.erase(thread_data);
    m_active_threads.push_back(thread_data);
    return false;
  }
}

/** 
 Workers "get next task" routine.

 A task can be handed over to the current thread directly during submit().
 if thread_var->m_wake_reason == WAKE_REASON_TASK.

 Or a task can be taken from the task queue.
 In case task queue is empty, the worker thread will park (wait for wakeup).
*/
bool thread_pool_generic::get_task(worker_data *thread_var, task *t)
{
  bool task_queue_was_full = false;
  std::unique_lock<std::mutex> lk(m_mtx);
  thread_var->m_is_executing_task = false;
  if (thread_var->m_is_long_task && m_long_tasks_count)
  {
    m_long_tasks_count--;
  }
  thread_var->m_is_long_task = false;

  if (m_task_queue.empty())
  {
    if (m_in_shutdown)
      return false;

    if (!wait_for_tasks(lk, thread_var))
      return false;

    /* Task was handed over directly.*/
    if (thread_var->m_wake_reason == WAKE_REASON_TASK)
    {
      *t= thread_var->m_task;
      thread_var->m_task.m_func= 0;
      goto end;
    }

    if (m_task_queue.empty())
      return false;
  }

  /* Dequeue from the task queue.*/
  task_queue_was_full= m_task_queue.full();
  *t= m_task_queue.front();
  m_task_queue.pop();
  m_tasks_dequeued++;

  if (task_queue_was_full)
  {
    /*
      There may be threads handing in submit(),
      because the task queue was full. Wake them
    */
    m_cv_queue_not_full.notify_all();
  }
end:
  thread_var->m_is_executing_task = true;
  thread_var->m_task_start_time = m_timestamp;
  return true;
}

/** Worker thread shutdown routine. */
void thread_pool_generic::worker_end(worker_data* thread_data)
{
  std::lock_guard<std::mutex> lk(m_mtx);
  m_active_threads.erase(thread_data);

  if (!thread_count() && m_in_shutdown)
  {
    /* Signal the destructor that no more threads are left. */
    m_cv_no_threads.notify_all();
  }
}

/* The worker get/execute task loop.*/
void thread_pool_generic::worker_main(worker_data *thread_var)
{
  task task;

  if(m_worker_init_callback)
   m_worker_init_callback();

  while (get_task(thread_var, &task))
  {
    task.m_func(task.m_arg);
  }

  if (m_worker_destroy_callback)
    m_worker_destroy_callback();

  worker_end(thread_var);
  m_thread_data_cache.put(thread_var);
}

void thread_pool_generic::timer_main()
{
  int last_tasks_dequeued= 0;
  size_t last_threads= 0;
  for (;;)
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    m_cv_timer.wait_for(lk, m_timer_interval);
    m_timestamp = std::chrono::system_clock::now();

    m_long_tasks_count = 0;

    if (!m_timer_on || (m_in_shutdown && m_task_queue.empty()))
      return;
    if (m_task_queue.empty())
      continue;

    for (auto thread_data = m_active_threads.front();
      thread_data;
      thread_data = thread_data->m_next)
    {
      if (thread_data->m_is_executing_task &&
        (thread_data->m_is_long_task || (m_timestamp - thread_data->m_task_start_time > LONG_TASK_DURATION)))
      {
        thread_data->m_is_long_task = true;
        m_long_tasks_count++;
      }
    }
    size_t thread_cnt = thread_count();
    if (m_active_threads.size() - m_long_tasks_count < m_concurrency)
    {
      wake_or_create_thread();
      continue;
    }


    if (last_tasks_dequeued == m_tasks_dequeued &&
        last_threads <= thread_cnt && m_active_threads.size() == thread_cnt)
    {
      // no progress made since last iteration. create new
      // thread
      add_thread();
    }
    last_tasks_dequeued= m_tasks_dequeued;
    last_threads= thread_cnt;
  }
}

/*
  Heuristic used for thread creation throttling.
  Returns interval in milliseconds between thread creation
  (depending on number of threads already in the pool, and 
  desired concurrency level)
*/
static int  throttling_interval_ms(size_t n_threads,size_t concurrency)
{
  if (n_threads < concurrency*4)
    return 0;

  if (n_threads < concurrency*8)
    return 50;

  if (n_threads < concurrency*16)
    return 100;

  return 200;
}

/* Create a new worker.*/
bool thread_pool_generic::add_thread()
{
  size_t n_threads = thread_count();

  if (n_threads >= m_max_threads)
    return false;

  if (n_threads >= m_min_threads)
  {
    auto now = std::chrono::system_clock::now();
    if (now - m_last_thread_creation <
      std::chrono::milliseconds(throttling_interval_ms(n_threads, m_concurrency)))
    {
      /* Throttle thread creation.*/
      return false;
    }
  }

  worker_data *thread_data = m_thread_data_cache.get();
  m_active_threads.push_back(thread_data);
  std::thread thread(&thread_pool_generic::worker_main,this, thread_data);
  m_last_thread_creation = std::chrono::system_clock::now();
  thread.detach();
  return true;
}

/** Wake a standby thread, and hand the given task over to this thread. */
bool thread_pool_generic::wake(worker_wake_reason reason, const task *t)
{
  assert(reason != WAKE_REASON_NONE);

  if (m_standby_threads.empty())
    return false;
  auto var= m_standby_threads.back();
  m_standby_threads.pop_back();
  m_active_threads.push_back(var);
  assert(var->m_wake_reason == WAKE_REASON_NONE);
  var->m_wake_reason= reason;
  var->m_cv.notify_one();
  if (t)
  {
    var->m_task= *t;
  }
  m_wakeups++;
  return true;
}

/** Start the timer thread*/
void thread_pool_generic::timer_start()
{
  m_timer_thread = std::thread(&thread_pool_generic::timer_main, this);
  m_timer_on = true;
}


/** Stop the timer thread*/
void thread_pool_generic::timer_stop()
{
  assert(m_in_shutdown || m_max_threads == m_min_threads);
  if(!m_timer_on)
    return;
  m_timer_on = false;
  m_cv_timer.notify_one();
  m_timer_thread.join();
}

thread_pool_generic::thread_pool_generic(int min_threads, int max_threads):
      m_thread_data_cache(max_threads),
      m_task_queue(10000),
      m_standby_threads(),
      m_active_threads(),
      m_mtx(),
      m_thread_timeout(std::chrono::milliseconds(60000)),
      m_timer_interval(std::chrono::milliseconds(100)),
      m_cv_no_threads(),
      m_cv_timer(),
      m_tasks_dequeued(),
      m_wakeups(),
      m_spurious_wakeups(),
      m_concurrency(std::thread::hardware_concurrency()),
      m_in_shutdown(),
      m_timer_on(),
      m_timestamp(),
      m_long_tasks_count(),
      m_last_thread_creation(),
      m_min_threads(min_threads),
      m_max_threads(max_threads)
{
  if (min_threads != max_threads)
    timer_start();
  if (m_max_threads < m_concurrency)
    m_concurrency = m_max_threads;
  if (m_min_threads > m_concurrency)
    m_concurrency = min_threads;
  if (!m_concurrency)
    m_concurrency = 1;
}


void thread_pool_generic::wake_or_create_thread()
{
  assert(!m_task_queue.empty());
  if (!m_standby_threads.empty())
  {
    task &t= m_task_queue.front();
    m_task_queue.pop();
    wake(WAKE_REASON_TASK, &t);
  }
  else
  {
    add_thread();
  }
}


/** Submit a new task*/
void thread_pool_generic::submit_task(const task &task)
{
  std::unique_lock<std::mutex> lk(m_mtx);

  while (m_task_queue.full())
  {
    m_cv_queue_not_full.wait(lk);
  }
  if (m_in_shutdown)
    return;
  m_task_queue.push(task);
  if (m_active_threads.size() - m_long_tasks_count < m_concurrency)
    wake_or_create_thread();
}

/**
  Wake  wake up all workers, and wait until they are gone
  Stop the timer.
*/
thread_pool_generic::~thread_pool_generic()
{
  std::unique_lock<std::mutex> lk(m_mtx);
  m_in_shutdown= true;
  /* Wake up idle threads. */
  while (wake(WAKE_REASON_SHUTDOWN))
  {
  }

  while (thread_count())
  {
    m_cv_no_threads.wait(lk);
  }

  lk.unlock();

  timer_stop();

  m_cv_queue_not_full.notify_all();
}

thread_pool *create_thread_pool_generic(int min_threads, int max_threads)
{ 
 return new thread_pool_generic(min_threads, max_threads);
}

} // namespace tpool
