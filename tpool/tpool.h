/* Copyright(C) 2019 MariaDB

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

#pragma once
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
/**
  Windows-specific native file handle struct.
  Apart from the actual handle, contains PTP_IO
  used by the Windows threadpool.
*/
struct native_file_handle
{
  HANDLE m_handle;
  PTP_IO m_ptp_io;
  native_file_handle(){};
  native_file_handle(HANDLE h) : m_handle(h), m_ptp_io() {}
  operator HANDLE() const { return m_handle; }
};
#else
#include <unistd.h>
typedef int native_file_handle;
#endif

namespace tpool
{
/**
 Task callback function
 */
typedef void (*callback_func)(void *);

/**
 Task, a void function with void *argument.
*/
struct task
{
  callback_func m_func;
  void *m_arg;
};

enum aio_opcode
{
  AIO_PREAD,
  AIO_PWRITE
};
const int MAX_AIO_USERDATA_LEN= 40;
struct aiocb;

/** IO callback function */
typedef void (*aio_callback_func)(const aiocb *cb, int ret_len, int err);

/** IO control block, includes parameters for the IO, and the callback*/
struct aiocb
{
  native_file_handle m_fh;
  aio_opcode m_opcode;
  unsigned long long m_offset;
  void *m_buffer;
  unsigned int m_len;
  aio_callback_func m_callback;
  void *m_internal;
  char m_userdata[MAX_AIO_USERDATA_LEN];
};

#ifdef _WIN32
struct win_aio_cb : OVERLAPPED
{
  aiocb m_aiocb;
};
#endif

/**
 AIO interface
*/
class aio
{
public:
  /** 
    Submit asyncronous IO.
    On completion, cb->m_callback is executed.
  */
  virtual int submit_io(const aiocb *cb)= 0;
  /** "Bind" file to AIO handler (used on Windows only) */
  virtual int bind(native_file_handle &fd)= 0;
  /** "Unind" file to AIO handler (used on Windows only) */
  virtual int unbind(const native_file_handle &fd)= 0;
  virtual ~aio(){};
};

class thread_pool;

extern aio *create_simulated_aio(thread_pool *tp, int max_io);


class thread_pool
{
protected:
  /* AIO handler */
  aio *m_aio;
  virtual aio* create_native_aio(int max_io) = 0;

  /** 
    Functions to be called at worker thread start/end
    can be used for example to set some TLS variables
  */
  void (*m_worker_init_callback)(void);
  void (*m_worker_destroy_callback)(void);

public:
  thread_pool() : m_aio(),m_worker_init_callback(),m_worker_destroy_callback() {}
  virtual void submit_task(const task &t)= 0;
  void set_thread_callbacks(void (*init)(), void (*destroy)())
  {
    m_worker_init_callback = init;
    m_worker_destroy_callback = destroy;
  }
  int configure_aio(bool use_native_aio, int max_io)
  {
    if (use_native_aio)
      m_aio= create_native_aio(max_io);
    if (!m_aio)
      m_aio= create_simulated_aio(this, max_io);
    return !m_aio ? -1 : 0;
  }
  int bind(native_file_handle &fd) { return m_aio->bind(fd); }
  void unbind(const native_file_handle &fd) { m_aio->unbind(fd); }
  int submit_io(const aiocb *cb) { return m_aio->submit_io(cb); }
  virtual ~thread_pool() { delete m_aio; }
};
const int DEFAULT_MIN_POOL_THREADS= 1;
const int DEFAULT_MAX_POOL_THHREADS= 500;
extern thread_pool *create_thread_pool_generic(int min_threads= DEFAULT_MIN_POOL_THREADS,
                                   int max_threads= DEFAULT_MAX_POOL_THHREADS);
#ifdef _WIN32
extern thread_pool *create_thread_pool_win(int min_threads= DEFAULT_MIN_POOL_THREADS,
                               int max_threads= DEFAULT_MAX_POOL_THHREADS);
#endif
} // namespace tpool
