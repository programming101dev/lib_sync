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
#include <p101_env/resource_classes.h>
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

#include <p101_thread/p101_pthread.h>
#include <p101_thread/p101_signal.h>
#include <stdio.h>

enum
{
    P101_THREAD_METADATA_SIZE = 8 + (sizeof(pthread_t) * 2) + 1,
    P101_MUTEX_OWNER_ID_SIZE  = P101_ENV_POINTER_RESOURCE_ID_SIZE + P101_THREAD_METADATA_SIZE + 1
};

/*
 * Builds the composite "<pointer>@<thread-metadata>" resource id shared by the
 * lock-held and lock-wait records, so an analyzer can attribute a held lock or
 * a blocking wait to the thread that owns it.
 */
static void pthread_track_thread_pointer_resource(const struct p101_env *env, p101_env_resource_kind event, const char *resource_class, const void *resource, const char *file_name, const char *function_name, int line_number)
{
    char      metadata[P101_THREAD_METADATA_SIZE];
    char      pointer_id[P101_ENV_POINTER_RESOURCE_ID_SIZE];
    char      resource_id[P101_MUTEX_OWNER_ID_SIZE];
    pthread_t thread;

    thread = p101_pthread_self(env);
    p101_pthread_resource_metadata(env, thread, metadata, sizeof(metadata));
    p101_env_pointer_resource_id(pointer_id, sizeof(pointer_id), resource);
    (void)snprintf(resource_id, sizeof(resource_id), "%s@%s", pointer_id, metadata);
    p101_env_track_resource(env, event, resource_class, resource_id, NULL, 0U, metadata, file_name, function_name, line_number);
}

#define P101_PTHREAD_TRACK_MUTEX_ACQUIRE(env, mutex) pthread_track_thread_pointer_resource((env), P101_ENV_RESOURCE_ACQUIRE, P101_RESOURCE_CLASS_PTHREAD_MUTEX_HELD, (const void *)(mutex), __FILE__, __func__, __LINE__)
#define P101_PTHREAD_TRACK_MUTEX_RELEASE(env, mutex) pthread_track_thread_pointer_resource((env), P101_ENV_RESOURCE_RELEASE, P101_RESOURCE_CLASS_PTHREAD_MUTEX_HELD, (const void *)(mutex), __FILE__, __func__, __LINE__)
#define P101_PTHREAD_TRACK_RWLOCK_ACQUIRE(env, rwlock) pthread_track_thread_pointer_resource((env), P101_ENV_RESOURCE_ACQUIRE, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_HELD, (const void *)(rwlock), __FILE__, __func__, __LINE__)
#define P101_PTHREAD_TRACK_RWLOCK_RELEASE(env, rwlock) pthread_track_thread_pointer_resource((env), P101_ENV_RESOURCE_RELEASE, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_HELD, (const void *)(rwlock), __FILE__, __func__, __LINE__)
#define P101_PTHREAD_TRACK_WAIT_ACQUIRE(env, resource_class, resource) pthread_track_thread_pointer_resource((env), P101_ENV_RESOURCE_ACQUIRE, (resource_class), (const void *)(resource), __FILE__, __func__, __LINE__)
#define P101_PTHREAD_TRACK_WAIT_RELEASE(env, resource_class, resource) pthread_track_thread_pointer_resource((env), P101_ENV_RESOURCE_RELEASE, (resource_class), (const void *)(resource), __FILE__, __func__, __LINE__)

/* cppcheck-suppress funcArgNamesDifferentUnnamed */
int p101_pthread_cond_broadcast(const struct p101_env *env, struct p101_error *err, pthread_cond_t *cond)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_cond_broadcast(cond);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_cond_destroy(const struct p101_env *env, struct p101_error *err, pthread_cond_t *cond)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_cond_destroy(cond);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_CONDITION, (const void *)cond, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_cond_init(const struct p101_env *env, struct p101_error *err, pthread_cond_t *restrict cond, const pthread_condattr_t *restrict attr)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_cond_init(cond, attr);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_CONDITION, (const void *)cond, 0U, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_cond_signal(const struct p101_env *env, struct p101_error *err, pthread_cond_t *cond)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_cond_signal(cond);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_cond_timedwait(const struct p101_env *env, struct p101_error *err, pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex, const struct timespec *restrict abstime)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    P101_PTHREAD_TRACK_MUTEX_RELEASE(env, mutex);
    P101_PTHREAD_TRACK_WAIT_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_CONDITION_WAIT, cond);
    errno   = 0;
    ret_val = pthread_cond_timedwait(cond, mutex, abstime);
    P101_PTHREAD_TRACK_WAIT_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_CONDITION_WAIT, cond);
    P101_PTHREAD_TRACK_MUTEX_ACQUIRE(env, mutex);

    if(ret_val != 0 && ret_val != ETIMEDOUT)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_cond_wait(const struct p101_env *env, struct p101_error *err, pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    P101_PTHREAD_TRACK_MUTEX_RELEASE(env, mutex);
    P101_PTHREAD_TRACK_WAIT_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_CONDITION_WAIT, cond);
    errno   = 0;
    ret_val = pthread_cond_wait(cond, mutex);
    P101_PTHREAD_TRACK_WAIT_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_CONDITION_WAIT, cond);
    P101_PTHREAD_TRACK_MUTEX_ACQUIRE(env, mutex);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_condattr_destroy(const struct p101_env *env, struct p101_error *err, pthread_condattr_t *attr)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_condattr_destroy(attr);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_CONDITION_ATTRIBUTES, (const void *)attr, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_condattr_init(const struct p101_env *env, struct p101_error *err, pthread_condattr_t *attr)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_condattr_init(attr);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_CONDITION_ATTRIBUTES, (const void *)attr, 0U, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutex_destroy(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *mutex)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_mutex_destroy(mutex);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_MUTEX, (const void *)mutex, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutex_init(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *restrict mutex, const pthread_mutexattr_t *restrict attr)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_mutex_init(mutex, attr);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_MUTEX, (const void *)mutex, 0U, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutex_lock(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *mutex)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    P101_PTHREAD_TRACK_WAIT_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_MUTEX_WAIT, mutex);
    errno   = 0;
    ret_val = pthread_mutex_lock(mutex);
    P101_PTHREAD_TRACK_WAIT_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_MUTEX_WAIT, mutex);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_PTHREAD_TRACK_MUTEX_ACQUIRE(env, mutex);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutex_trylock(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *mutex)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    P101_PTHREAD_TRACK_WAIT_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_MUTEX_WAIT, mutex);
    errno   = 0;
    ret_val = pthread_mutex_trylock(mutex);
    P101_PTHREAD_TRACK_WAIT_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_MUTEX_WAIT, mutex);

    if(ret_val != 0 && ret_val != EBUSY)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else if(ret_val == 0)
    {
        P101_PTHREAD_TRACK_MUTEX_ACQUIRE(env, mutex);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutex_unlock(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *mutex)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_mutex_unlock(mutex);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_PTHREAD_TRACK_MUTEX_RELEASE(env, mutex);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_destroy(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_mutexattr_destroy(attr);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_MUTEX_ATTRIBUTES, (const void *)attr, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_gettype(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict type)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_mutexattr_gettype(attr, type);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_init(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_mutexattr_init(attr);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_MUTEX_ATTRIBUTES, (const void *)attr, 0U, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_settype(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int type)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_mutexattr_settype(attr, type);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_once(const struct p101_env *env, struct p101_error *err, pthread_once_t *once_control, void (*init_routine)(void))
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_once(once_control, init_routine);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlock_destroy(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_rwlock_destroy(rwlock);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK, (const void *)rwlock, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlock_init(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *restrict rwlock, const pthread_rwlockattr_t *restrict attr)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_rwlock_init(rwlock, attr);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK, (const void *)rwlock, 0U, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlock_rdlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    P101_PTHREAD_TRACK_WAIT_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_READ_WAIT, rwlock);
    errno   = 0;
    ret_val = pthread_rwlock_rdlock(rwlock);
    P101_PTHREAD_TRACK_WAIT_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_READ_WAIT, rwlock);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_PTHREAD_TRACK_RWLOCK_ACQUIRE(env, rwlock);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlock_tryrdlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    P101_PTHREAD_TRACK_WAIT_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_READ_WAIT, rwlock);
    errno   = 0;
    ret_val = pthread_rwlock_tryrdlock(rwlock);
    P101_PTHREAD_TRACK_WAIT_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_READ_WAIT, rwlock);

    if(ret_val != 0 && ret_val != EBUSY)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else if(ret_val == 0)
    {
        P101_PTHREAD_TRACK_RWLOCK_ACQUIRE(env, rwlock);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlock_trywrlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    P101_PTHREAD_TRACK_WAIT_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_WRITE_WAIT, rwlock);
    errno   = 0;
    ret_val = pthread_rwlock_trywrlock(rwlock);
    P101_PTHREAD_TRACK_WAIT_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_WRITE_WAIT, rwlock);

    if(ret_val != 0 && ret_val != EBUSY)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else if(ret_val == 0)
    {
        P101_PTHREAD_TRACK_RWLOCK_ACQUIRE(env, rwlock);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlock_unlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_rwlock_unlock(rwlock);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_PTHREAD_TRACK_RWLOCK_RELEASE(env, rwlock);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlock_wrlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    P101_PTHREAD_TRACK_WAIT_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_WRITE_WAIT, rwlock);
    errno   = 0;
    ret_val = pthread_rwlock_wrlock(rwlock);
    P101_PTHREAD_TRACK_WAIT_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_WRITE_WAIT, rwlock);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_PTHREAD_TRACK_RWLOCK_ACQUIRE(env, rwlock);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlockattr_destroy(const struct p101_env *env, struct p101_error *err, pthread_rwlockattr_t *attr)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_rwlockattr_destroy(attr);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_RELEASE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_ATTRIBUTES, (const void *)attr, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlockattr_init(const struct p101_env *env, struct p101_error *err, pthread_rwlockattr_t *attr)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    errno   = 0;
    ret_val = pthread_rwlockattr_init(attr);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }
    else
    {
        P101_TRACK_POINTER_RESOURCE_ACQUIRE(env, P101_RESOURCE_CLASS_PTHREAD_RWLOCK_ATTRIBUTES, (const void *)attr, 0U, NULL);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

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

int p101_pthread_condattr_getpshared(const struct p101_env *env, struct p101_error *err, const pthread_condattr_t *restrict attr, int *restrict pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_condattr_getpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_condattr_setpshared(const struct p101_env *env, struct p101_error *err, pthread_condattr_t *attr, int pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_condattr_setpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutex_getprioceiling(const struct p101_env *env, struct p101_error *err, const pthread_mutex_t *restrict mutex, int *restrict prioceiling)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_mutex_getprioceiling(mutex, prioceiling);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutex_setprioceiling(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *restrict mutex, int prioceiling, int *restrict old_ceiling)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_mutex_setprioceiling(mutex, prioceiling, old_ceiling);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_getprioceiling(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict prioceiling)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_mutexattr_getprioceiling(attr, prioceiling);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_getprotocol(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict protocol)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_mutexattr_getprotocol(attr, protocol);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_getpshared(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_mutexattr_getpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_setprioceiling(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int prioceiling)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_mutexattr_setprioceiling(attr, prioceiling);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_setprotocol(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int protocol)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_mutexattr_setprotocol(attr, protocol);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_mutexattr_setpshared(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_mutexattr_setpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlockattr_getpshared(const struct p101_env *env, struct p101_error *err, const pthread_rwlockattr_t *restrict attr, int *restrict pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_rwlockattr_getpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}

int p101_pthread_rwlockattr_setpshared(const struct p101_env *env, struct p101_error *err, pthread_rwlockattr_t *attr, int pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err, ret_val);
    ret_val = pthread_rwlockattr_setpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_WRAPPER_DONE(env);
    return ret_val;
}
