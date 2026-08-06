/*
 * Copyright 2026 D'Arcy Smith.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "p101_sync/p101_pthread.h"
#include "p101_sync/p101_semaphore.h"
#include <p101_env/wrapper.h>

/*
 * Copyright 2021-2024 D'Arcy Smith.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

enum
{
    RESOURCE_ID_SIZE = 64
};

#include <fcntl.h>
#include <stdarg.h>

static bool   sem_open_uses_create_args(int oflag);
static mode_t sem_open_mode_arg(va_list *args);

static bool sem_open_uses_create_args(int oflag)
{
    bool uses_arg;

    if((oflag & O_CREAT) == O_CREAT)
    {
        uses_arg = true;
    }
    else
    {
        uses_arg = false;
    }

    return uses_arg;
}

static mode_t sem_open_mode_arg(va_list *args)
{
    mode_t mode;

#if defined(__APPLE__) || defined(__FreeBSD__)
    mode = (mode_t)va_arg(*args, int);
#else
    mode = va_arg(*args, mode_t);
#endif

    return mode;
}

int p101_sem_close(const struct p101_env *env, struct p101_error *err, sem_t *sem)
{
    int  ret_val;
    char resource_id[RESOURCE_ID_SIZE];

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN(env, err, ret_val, -1);
    p101_env_pointer_resource_id(resource_id, sizeof(resource_id), sem);
    errno   = 0;
    ret_val = sem_close(sem);

    if(ret_val == -1)
    {
        P101_ERROR_RAISE_ERRNO(err, errno);
    }
    else
    {
        P101_TRACK_RESOURCE_RELEASE(env, "named-semaphore", resource_id, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

sem_t *p101_sem_open(const struct p101_env *env, struct p101_error *err, const char *name, int oflag, ...)
{
    sem_t  *ret_val;
    bool    uses_create_arguments;
    va_list args;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN(env, err, ret_val, SEM_FAILED);
    errno = 0;

    uses_create_arguments = sem_open_uses_create_args(oflag);
    if(uses_create_arguments)
    {
        mode_t       mode;
        unsigned int value;

        va_start(args, oflag);
        mode  = sem_open_mode_arg(&args);
        value = va_arg(args, unsigned int);
        va_end(args);

        ret_val = sem_open(name, oflag, mode, value);
    }
    else
    {
        ret_val = sem_open(name, oflag);
    }

    if(ret_val == SEM_FAILED)
    {
        P101_ERROR_RAISE_ERRNO(err, errno);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_ACQUIRE(env, "named-semaphore", ret_val, 0U, name);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_sem_post(const struct p101_env *env, struct p101_error *err, sem_t *sem)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN(env, err, ret_val, -1);
    errno   = 0;
    ret_val = sem_post(sem);

    if(ret_val == -1)
    {
        P101_ERROR_RAISE_ERRNO(err, errno);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_sem_trywait(const struct p101_env *env, struct p101_error *err, sem_t *sem)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN(env, err, ret_val, -1);
    errno   = 0;
    ret_val = sem_trywait(sem);

    if(ret_val == -1 && errno != EAGAIN)
    {
        P101_ERROR_RAISE_ERRNO(err, errno);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_sem_unlink(const struct p101_env *env, struct p101_error *err, const char *name)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN(env, err, ret_val, -1);
    errno   = 0;
    ret_val = sem_unlink(name);

    if(ret_val == -1)
    {
        P101_ERROR_RAISE_ERRNO(err, errno);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_sem_wait(const struct p101_env *env, struct p101_error *err, sem_t *sem)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN(env, err, ret_val, -1);
    errno   = 0;
    ret_val = sem_wait(sem);

    if(ret_val == -1)
    {
        P101_ERROR_RAISE_ERRNO(err, errno);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}
