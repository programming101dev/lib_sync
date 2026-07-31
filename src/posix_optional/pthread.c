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

#include "p101_sync/sync.h"
#include <p101_env/wrapper.h>

int p101_pthread_condattr_getpshared(const struct p101_env *env, struct p101_error *err, const pthread_condattr_t *restrict attr, int *restrict pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_condattr_getpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_condattr_setpshared(const struct p101_env *env, struct p101_error *err, pthread_condattr_t *attr, int pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_condattr_setpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_mutex_getprioceiling(const struct p101_env *env, struct p101_error *err, const pthread_mutex_t *restrict mutex, int *restrict prioceiling)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_mutex_getprioceiling(mutex, prioceiling);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_mutex_setprioceiling(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *restrict mutex, int prioceiling, int *restrict old_ceiling)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_mutex_setprioceiling(mutex, prioceiling, old_ceiling);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_mutexattr_getprioceiling(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict prioceiling)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_mutexattr_getprioceiling(attr, prioceiling);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_mutexattr_getprotocol(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict protocol)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_mutexattr_getprotocol(attr, protocol);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_mutexattr_getpshared(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_mutexattr_getpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_mutexattr_setprioceiling(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int prioceiling)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_mutexattr_setprioceiling(attr, prioceiling);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_mutexattr_setprotocol(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int protocol)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_mutexattr_setprotocol(attr, protocol);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_mutexattr_setpshared(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_mutexattr_setpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_rwlockattr_getpshared(const struct p101_env *env, struct p101_error *err, const pthread_rwlockattr_t *restrict attr, int *restrict pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_rwlockattr_getpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}

int p101_pthread_rwlockattr_setpshared(const struct p101_env *env, struct p101_error *err, pthread_rwlockattr_t *attr, int pshared)
{
    int ret_val;

    P101_TRACE(env);
    P101_WRAPPER_FAULT_RETURN_CODE(env, err);
    ret_val = pthread_rwlockattr_setpshared(attr, pshared);

    if(ret_val != 0)
    {
        P101_ERROR_RAISE_ERRNO(err, ret_val);
    }

    P101_TRACE_EXIT(env);
    return ret_val;
}
