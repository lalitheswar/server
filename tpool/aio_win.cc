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
#include <algorithm>
#include <assert.h>
#include <condition_variable>
#include <iostream>
#include <limits.h>
#include <mutex>
#include <queue>
#include <stack>
#include <thread>
#include <vector>
#include <tpool.h>

namespace tpool
{

/*
  Windows AIO implementation, completion port based.
  A single thread collects the completion notification with
  GetQueuedCompletionStatus(), and forwards io completion callback
  the worker threadpool
*/
class tpool_generic_win_aio : public aio
{

  struct generic_win_aiocb : win_aio_cb
  {
    int m_err;
    int m_ret_len;
  };
  /* AIO control block cache.*/
  cache<generic_win_aiocb> m_cache;

  /* Thread that does collects completion status from the completion port. */
  std::thread m_thread;

  /* IOCP Completion port.*/
  HANDLE m_completion_port;

  /* The worker pool where completion routine is executed, as task. */
  thread_pool* m_pool;
public:
 
  tpool_generic_win_aio(thread_pool* pool, int max_io) : m_pool(pool), m_cache(max_io)
  {
    m_completion_port = CreateIoCompletionPort(INVALID_HANDLE_VALUE, 0, 0, 0);
    m_thread = std::thread(aio_completion_thread_proc, this);
  }

  void io_completion(generic_win_aiocb* cb)
  {
    cb->m_aiocb.m_callback(&cb->m_aiocb, cb->m_ret_len, cb->m_err);
    m_cache.put(cb);
  }

  /**
   Task to be executed in the work pool.
  */
  static void io_completion_task(void* data)
  {
    auto cb = (generic_win_aiocb*)data;
    auto aio = (tpool_generic_win_aio*)cb->m_aiocb.m_internal;
    aio->io_completion(cb);
  }

  void completion_thread_work()
  {
    for (;;)
    {
      DWORD n_bytes;
      generic_win_aiocb* win_aiocb;
      ULONG_PTR key;
      if (!GetQueuedCompletionStatus(m_completion_port, &n_bytes, &key,
        (LPOVERLAPPED*)& win_aiocb, INFINITE))
        break;

      win_aiocb->m_err = 0;
      win_aiocb->m_ret_len = n_bytes;

      if (n_bytes != win_aiocb->m_aiocb.m_len)
      {
        if (GetOverlappedResult(win_aiocb->m_aiocb.m_fh, win_aiocb,
          (LPDWORD)& win_aiocb->m_ret_len, FALSE))
        {
          win_aiocb->m_err = GetLastError();
        }
      }
      m_pool->submit_task({ io_completion_task, win_aiocb });
    }
  }

  static void aio_completion_thread_proc(tpool_generic_win_aio* aio)
  {
    aio->completion_thread_work();
  }

  ~tpool_generic_win_aio()
  {
    if (m_completion_port)
      CloseHandle(m_completion_port);
    m_thread.join();
  }

  virtual int submit_io(const aiocb* aiocb) override
  {
    generic_win_aiocb* cb = m_cache.get();
    memset(cb, 0, sizeof(OVERLAPPED));
    cb->m_aiocb = *aiocb;
    cb->m_aiocb.m_internal = this;

    ULARGE_INTEGER uli;
    uli.QuadPart = aiocb->m_offset;
    cb->Offset = uli.LowPart;
    cb->OffsetHigh = uli.HighPart;

    BOOL ok;
    if (aiocb->m_opcode == AIO_PREAD)
      ok = ReadFile(aiocb->m_fh.m_handle, aiocb->m_buffer, aiocb->m_len, 0, cb);
    else
      ok = WriteFile(aiocb->m_fh.m_handle, aiocb->m_buffer, aiocb->m_len, 0, cb);

    if (ok || (GetLastError() == ERROR_IO_PENDING))
      return 0;
    return -1;
  }

  // Inherited via aio
  virtual int bind(native_file_handle& fd) override
  {
    return CreateIoCompletionPort(fd, m_completion_port, 0, 0) ? 0
      : GetLastError();
  }
  virtual int unbind(const native_file_handle& fd) override { return 0; }
};

aio* create_win_aio(thread_pool* pool, int max_io)
{
  return new tpool_generic_win_aio(pool, max_io);
}

} // namespace tpool
